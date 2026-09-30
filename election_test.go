// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pb "go.etcd.io/raft/v3/raftpb"
)

// voteCluster is a minimal, deterministic, in-memory three-node test fixture
// used to exercise candidate ballot tracking. Messages are delivered
// explicitly (via step), so delayed/stale responses can be injected at will.
type voteCluster struct {
	t     *testing.T
	peers map[uint64]*raft
}

func newVoteCluster(t *testing.T, preVote bool) *voteCluster {
	peers := []uint64{1, 2, 3}
	c := &voteCluster{t: t, peers: map[uint64]*raft{}}
	for _, id := range peers {
		cfg := newTestConfig(id, 10, 1, newTestMemoryStorage(withPeers(peers...)))
		cfg.PreVote = preVote
		c.peers[id] = newRaft(cfg)
	}
	return c
}

// step delivers a single message and flushes the recipient's durable queue,
// returning all new outbound messages (not delivered further).
func (c *voteCluster) step(m *pb.Message) []*pb.Message {
	c.t.Helper()
	r := c.peers[m.GetTo()]
	assert.NotNil(c.t, r)
	assert.NoError(c.t, r.Step(m))
	r.advanceMessagesAfterAppend()
	return r.readMessages()
}

// campaignWith starts a campaign on id with the given type and returns the
// request messages addressed to the other peers. The self vote is delivered
// through the durable queue as part of this call.
func (c *voteCluster) campaignWith(id uint64, typ CampaignType) map[uint64]*pb.Message {
	c.t.Helper()
	r := c.peers[id]
	r.campaign(typ)
	r.advanceMessagesAfterAppend()
	reqs := map[uint64]*pb.Message{}
	for _, m := range r.readMessages() {
		reqs[m.GetTo()] = m
	}
	return reqs
}

// deliverVote feeds a vote request req to its target voter and returns the
// voter's response back to the candidate. When reject is true, the response is
// forced to a rejection (carrying the voter's current term), simulating a
// log/term conflict.
func (c *voteCluster) deliverVote(cand uint64, req *pb.Message, reject bool) *pb.Message {
	c.t.Helper()
	for _, m := range c.step(req) {
		if m.GetTo() == cand &&
			(m.GetType() == pb.MsgVoteResp || m.GetType() == pb.MsgPreVoteResp) {
			if reject {
				m.Reject = new(true)
				m.Term = new(c.peers[m.GetFrom()].Term)
			}
			return m
		}
	}
	return nil
}

func assertOutcomes(t *testing.T, s BallotSummary, want map[uint64]BallotOutcome) {
	t.Helper()
	assert.Equal(t, len(want), len(s.Ballots), "ballot count mismatch: %+v", s.Ballots)
	gotIDs := make([]uint64, 0, len(s.Ballots))
	for _, b := range s.Ballots {
		gotIDs = append(gotIDs, b.ID)
		assert.Equal(t, want[b.ID], b.Outcome, "unexpected outcome for peer %x", b.ID)
		assert.Equal(t, s.Term, b.Term, "ballot term mismatch for peer %x", b.ID)
	}
	assert.True(t, sortedUint64(gotIDs), "ballots must be sorted by ID: %v", gotIDs)
}

func sortedUint64(ids []uint64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i-1] > ids[i] {
			return false
		}
	}
	return true
}

// voteResp is a small constructor for an inbound vote response.
func voteResp(from, to, term uint64, typ pb.MessageType, reject bool) *pb.Message {
	m := &pb.Message{
		From: new(from), To: new(to), Term: new(term), Type: typ.Enum(),
	}
	if reject {
		m.Reject = new(true)
	}
	return m
}

// TestBallotSummaryThreeNodes drives a three-node election to completion and
// checks the grant / pending / reject summary, the stable ID ordering, the
// term, and the candidate -> leader transition. It also injects delayed
// responses from an older term and asserts they cannot change anything.
func TestBallotSummaryThreeNodes(t *testing.T) {
	c := newVoteCluster(t, false /* preVote */)
	n1 := c.peers[1]

	// Initially no election round is tracked.
	assert.True(t, n1.ballotSummary().Empty())

	// Node 1 starts a campaign for term 1. Its self vote is persisted and
	// counted immediately; peers 2 and 3 have not responded yet.
	reqs := c.campaignWith(1, campaignElection)
	assert.Equal(t, StateCandidate, n1.state)
	assert.Equal(t, uint64(1), n1.Term)
	assertOutcomes(t, n1.ballotSummary(), map[uint64]BallotOutcome{
		1: BallotGranted, 2: BallotPending, 3: BallotPending,
	})

	// Peer 2 grants: self + peer 2 form a majority, node 1 becomes leader.
	c.step(c.deliverVote(1, reqs[2], false))
	assert.Equal(t, StateLeader, n1.state)
	assert.Equal(t, uint64(1), n1.Term)
	assert.Equal(t, uint64(1), n1.lead)

	s := n1.ballotSummary()
	assert.Equal(t, uint64(1), s.Term)
	assert.Equal(t, []uint64{1, 2}, s.Granted())
	assert.Empty(t, s.Rejected())
	assert.Equal(t, []uint64{3}, s.Pending())

	// Peer 3's grant only arrives after node 1 already won. A leader ignores
	// vote responses, so peer 3 remains reported as pending (no response was
	// counted while campaigning).
	c.step(c.deliverVote(1, reqs[3], false))
	assert.Equal(t, StateLeader, n1.state)
	assert.Equal(t, []uint64{3}, n1.ballotSummary().Pending())

	// A delayed GRANT response carrying an OLDER term (term 0) arrives late.
	// It must be discarded by the term guard and must not change term, state
	// or summary.
	c.step(voteResp(3, 1, 0, pb.MsgVoteResp, false))
	assert.Equal(t, StateLeader, n1.state)
	assert.Equal(t, uint64(1), n1.Term)
	assert.Equal(t, []uint64{1, 2}, n1.ballotSummary().Granted())
	assert.Equal(t, []uint64{3}, n1.ballotSummary().Pending())
}

// TestBallotSummaryStaleTermAndNewTerm verifies that:
//   - a vote response from an older term cannot rewrite the current summary;
//   - entering a new term clears the old ballots before a fresh round is
//     tracked, and the leader transition stays consistent.
func TestBallotSummaryStaleTermAndNewTerm(t *testing.T) {
	c := newVoteCluster(t, false /* preVote */)
	n1 := c.peers[1]

	// Term 1 campaign: node 1 votes for itself, both peers reject.
	reqs := c.campaignWith(1, campaignElection)
	assert.Equal(t, uint64(1), n1.ballotSummary().Term)
	c.step(c.deliverVote(1, reqs[2], true))
	c.step(c.deliverVote(1, reqs[3], true))

	// Two rejections => election lost, node steps down to a follower while
	// staying in term 1. The just-finished round's summary is still queryable
	// (self granted, both peers rejected).
	assert.Equal(t, StateFollower, n1.state)
	assert.Equal(t, uint64(1), n1.Term)
	assert.Equal(t, []uint64{1}, n1.ballotSummary().Granted())
	assert.Equal(t, []uint64{2, 3}, n1.ballotSummary().Rejected())

	// A delayed GRANT from the finished term 1 arrives while node 1 is a
	// follower. It is ignored and must NOT rewrite the summary (in particular,
	// peer 2 stays rejected and the self grant is unchanged).
	c.step(voteResp(2, 1, 1, pb.MsgVoteResp, false))
	assert.Equal(t, StateFollower, n1.state)
	assert.Equal(t, []uint64{1}, n1.ballotSummary().Granted())
	assert.Equal(t, []uint64{2, 3}, n1.ballotSummary().Rejected())

	// Node 1 starts a new campaign for term 2. Entering the new term cleared
	// the old ballots; the fresh round starts fully pending (except self).
	reqs2 := c.campaignWith(1, campaignElection)
	assert.Equal(t, StateCandidate, n1.state)
	assert.Equal(t, uint64(2), n1.Term)
	assertOutcomes(t, n1.ballotSummary(), map[uint64]BallotOutcome{
		1: BallotGranted, 2: BallotPending, 3: BallotPending,
	})

	// A delayed response belonging to the PREVIOUS term 1 is injected. It is
	// dropped by the term guard and must not flip node 2's pending ballot.
	c.step(voteResp(2, 1, 1, pb.MsgVoteResp, false))
	assert.Equal(t, StateCandidate, n1.state)
	assert.Equal(t, uint64(2), n1.Term)
	assert.Equal(t, []uint64{2, 3}, n1.ballotSummary().Pending())

	// The genuine term-2 response from peer 2 wins the election.
	c.step(c.deliverVote(1, reqs2[2], false))
	assert.Equal(t, StateLeader, n1.state)
	assert.Equal(t, uint64(2), n1.Term)
	assert.Equal(t, uint64(1), n1.lead)
	s := n1.ballotSummary()
	assert.Equal(t, uint64(2), s.Term)
	assert.Equal(t, []uint64{1, 2}, s.Granted())
	assert.Equal(t, []uint64{3}, s.Pending())
	assert.Empty(t, s.Rejected())

	// A heartbeat with a higher term steps node 1 down into a follower at the
	// new term, which clears the summary.
	c.step(&pb.Message{
		From: new(uint64(3)), To: new(uint64(1)),
		Term: new(uint64(3)), Type: pb.MsgHeartbeat.Enum(),
	})
	assert.Equal(t, StateFollower, n1.state)
	assert.Equal(t, uint64(3), n1.Term)
	assert.True(t, n1.ballotSummary().Empty())
}

// TestBallotSummaryPreVote covers the PreVote round: grants carry the future
// term while rejections carry the node's current term, and winning PreVote
// starts a fresh real-election round.
func TestBallotSummaryPreVote(t *testing.T) {
	c := newVoteCluster(t, true /* preVote */)
	n1 := c.peers[1]

	// PreVote does not bump the term; the round is tracked at the future term.
	reqs := c.campaignWith(1, campaignPreElection)
	assert.Equal(t, StatePreCandidate, n1.state)
	assert.Equal(t, uint64(0), n1.Term)
	assert.Equal(t, uint64(1), n1.ballotSummary().Term)
	assertOutcomes(t, n1.ballotSummary(), map[uint64]BallotOutcome{
		1: BallotGranted, 2: BallotPending, 3: BallotPending,
	})

	// Peer 3 rejects with the current term 0; it is recorded as rejected and
	// does not bump the candidate's term.
	c.step(c.deliverVote(1, reqs[3], true))
	assert.Equal(t, StatePreCandidate, n1.state)
	assert.Equal(t, uint64(0), n1.Term)
	assert.Equal(t, []uint64{3}, n1.ballotSummary().Rejected())

	// Peer 2 grants PreVote at future term 1: self + peer 2 win PreVote, node
	// increments to term 1 and starts the real election with a fresh summary.
	c.step(c.deliverVote(1, reqs[2], false))
	assert.Equal(t, StateCandidate, n1.state)
	assert.Equal(t, uint64(1), n1.Term)
	assertOutcomes(t, n1.ballotSummary(), map[uint64]BallotOutcome{
		1: BallotGranted, 2: BallotPending, 3: BallotPending,
	})
}

// TestRawNodeBallotSummary verifies the public RawNode accessor returns the
// same snapshot as the internal view during a campaign.
func TestRawNodeBallotSummary(t *testing.T) {
	peers := []uint64{1, 2, 3}
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(peers...)))
	rn := newTestRawNode(1, 10, 1, cfg.Storage)
	r := rn.raft

	assert.True(t, rn.BallotSummary().Empty())
	r.campaign(campaignElection)
	r.advanceMessagesAfterAppend()
	for range r.readMessages() {
	}

	s := rn.BallotSummary()
	assert.Equal(t, uint64(1), s.Term)
	assert.Equal(t, []uint64{1}, s.Granted())
	assert.Equal(t, []uint64{2, 3}, s.Pending())
}
