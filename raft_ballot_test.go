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

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// ballotOf returns the current ballot term and a map of node ID to vote
// state, for concise assertions.
func ballotOf(t *testing.T, r *raft) (uint64, map[uint64]tracker.VoteState) {
	t.Helper()
	s := r.trk.BallotSummary()
	m := make(map[uint64]tracker.VoteState, len(s.Votes))
	for _, v := range s.Votes {
		m[v.ID] = v.State
	}
	return s.Term, m
}

// assertBallot asserts that the current ballot is for the given term and
// contains exactly the given per-voter states.
func assertBallot(t *testing.T, r *raft, term uint64, want map[uint64]tracker.VoteState) {
	t.Helper()
	gotTerm, got := ballotOf(t, r)
	require.Equal(t, term, gotTerm, "ballot term")
	require.Equal(t, want, got, "ballot votes")
}

// assertBallotEmpty asserts that no election is in progress: the summary
// carries no term and no votes.
func assertBallotEmpty(t *testing.T, r *raft) {
	t.Helper()
	gotTerm, got := ballotOf(t, r)
	require.Equal(t, uint64(0), gotTerm, "ballot term")
	require.Empty(t, got, "ballot votes")
}

// TestCandidateBallotSummary constructs a three-node election in which the
// candidate's first bid fails and a delayed response from the old term
// arrives during the second bid. It verifies that:
//   - the ballot summary is queryable, carries the current term and lists
//     every voter as granted, rejected or not-responded in stable ID order;
//   - delayed messages from an old term cannot rewrite the current summary;
//   - entering a new term clears the old votes;
//   - the summary, term and leader transition stay consistent: once the
//     candidate wins the election and becomes leader, the settled ballot is
//     cleared.
func TestCandidateBallotSummary(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	// --- Term 1: node 1 starts an election. ---
	r.Step(&pb.Message{From: new(uint64(1)), To: new(uint64(1)), Type: pb.MsgHup.Enum()})
	r.advanceMessagesAfterAppend()
	require.Equal(t, StateCandidate, r.state)
	require.Equal(t, uint64(1), r.Term)

	// The self vote is recorded; the peers have not responded yet.
	assertBallot(t, r, 1, map[uint64]tracker.VoteState{
		1: tracker.VoteGranted,
		2: tracker.VoteNotResponded,
		3: tracker.VoteNotResponded,
	})

	// Node 2 rejects: one grant, one rejection, one pending.
	r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Type: pb.MsgVoteResp.Enum(), Reject: new(true)})
	assertBallot(t, r, 1, map[uint64]tracker.VoteState{
		1: tracker.VoteGranted,
		2: tracker.VoteRejected,
		3: tracker.VoteNotResponded,
	})

	// Node 3 rejects as well: the election is lost. Node 1 returns to
	// follower at term 1 and the old ballot is cleared.
	r.Step(&pb.Message{From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(1)), Type: pb.MsgVoteResp.Enum(), Reject: new(true)})
	require.Equal(t, StateFollower, r.state)
	require.Equal(t, uint64(1), r.Term)
	assertBallotEmpty(t, r)

	// --- Term 2: node 1 starts a new election. ---
	r.Step(&pb.Message{From: new(uint64(1)), To: new(uint64(1)), Type: pb.MsgHup.Enum()})
	r.advanceMessagesAfterAppend()
	require.Equal(t, StateCandidate, r.state)
	require.Equal(t, uint64(2), r.Term)

	// Entering the new term cleared the old votes: only the self vote is on
	// the new ballot, and the term is the new one.
	assertBallot(t, r, 2, map[uint64]tracker.VoteState{
		1: tracker.VoteGranted,
		2: tracker.VoteNotResponded,
		3: tracker.VoteNotResponded,
	})

	// Delayed responses from the previous (term 1) election arrive. Whether
	// they grant or reject, they must not rewrite the current (term 2)
	// summary.
	r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Type: pb.MsgVoteResp.Enum()})
	r.Step(&pb.Message{From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(1)), Type: pb.MsgVoteResp.Enum(), Reject: new(true)})
	// A response carrying term 0 is stale as well; the tracker ignores it.
	r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(0)), Type: pb.MsgVoteResp.Enum()})
	assertBallot(t, r, 2, map[uint64]tracker.VoteState{
		1: tracker.VoteGranted,
		2: tracker.VoteNotResponded,
		3: tracker.VoteNotResponded,
	})

	// Fresh term-2 responses arrive: both grant, giving a quorum. Node 1
	// becomes leader at term 2; the settled election clears the ballot.
	r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Type: pb.MsgVoteResp.Enum()})
	r.Step(&pb.Message{From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(2)), Type: pb.MsgVoteResp.Enum()})
	require.Equal(t, StateLeader, r.state)
	require.Equal(t, uint64(2), r.Term)
	assertBallotEmpty(t, r)
}

// TestCandidateBallotSummaryViaStatus checks that the ballot is exposed
// through the public Status view while a candidacy is in progress.
func TestCandidateBallotSummaryViaStatus(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))

	r.Step(&pb.Message{From: new(uint64(1)), To: new(uint64(1)), Type: pb.MsgHup.Enum()})
	r.advanceMessagesAfterAppend()

	status := getStatus(r)
	require.Equal(t, StateCandidate, status.RaftState)
	require.Equal(t, uint64(1), status.Ballot.Term)
	require.Equal(t, []uint64{1, 2, 3}, voterIDs(status.Ballot))
	require.Equal(t, tracker.VoteGranted, status.Ballot.Votes[0].State)
	require.Equal(t, tracker.VoteNotResponded, status.Ballot.Votes[1].State)
	require.Equal(t, tracker.VoteNotResponded, status.Ballot.Votes[2].State)
}

func voterIDs(s tracker.BallotSummary) []uint64 {
	ids := make([]uint64, len(s.Votes))
	for i, v := range s.Votes {
		ids[i] = v.ID
	}
	return ids
}

// TestPreCandidateBallotSummary checks that the ballot summary also works
// during a pre-vote candidacy. A pre-candidate polls one term ahead, but
// rejection responses carry the responder's term (which may be older than
// the polled term); such responses must still be recorded because they are
// not older than the candidacy's own term.
func TestPreCandidateBallotSummary(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.PreVote = true
	r := newRaft(cfg)
	// Node 1 already serves at term 1 (e.g. it led a prior term).
	r.loadState(&pb.HardState{Term: new(uint64(1))})

	// Node 1 starts a pre-vote election at term 1, polling for term 2.
	// Pre-vote does not increment r.Term.
	r.Step(&pb.Message{From: new(uint64(1)), To: new(uint64(1)), Type: pb.MsgHup.Enum()})
	r.advanceMessagesAfterAppend()
	require.Equal(t, StatePreCandidate, r.state)
	require.Equal(t, uint64(1), r.Term)

	// The self vote is recorded; the summary carries the candidacy's term.
	assertBallot(t, r, 1, map[uint64]tracker.VoteState{
		1: tracker.VoteGranted,
		2: tracker.VoteNotResponded,
		3: tracker.VoteNotResponded,
	})

	// A pre-vote rejection carries the responder's term (1), which is older
	// than the polled term (2) but equal to the candidacy's term. It must be
	// recorded rather than discarded as stale.
	r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Type: pb.MsgPreVoteResp.Enum(), Reject: new(true)})
	assertBallot(t, r, 1, map[uint64]tracker.VoteState{
		1: tracker.VoteGranted,
		2: tracker.VoteRejected,
		3: tracker.VoteNotResponded,
	})

	// A second rejection defeats the pre-vote; node 1 returns to follower at
	// term 1 and the settled ballot is cleared.
	r.Step(&pb.Message{From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(1)), Type: pb.MsgPreVoteResp.Enum(), Reject: new(true)})
	require.Equal(t, StateFollower, r.state)
	require.Equal(t, uint64(1), r.Term)
	assertBallotEmpty(t, r)
}
