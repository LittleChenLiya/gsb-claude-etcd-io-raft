// Copyright 2019 The etcd Authors
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

package tracker

import (
	"fmt"
	"slices"
	"strings"

	"go.etcd.io/raft/v3/quorum"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Config reflects the configuration tracked in a ProgressTracker.
type Config struct {
	Voters quorum.JointConfig
	// AutoLeave is true if the configuration is joint and a transition to the
	// incoming configuration should be carried out automatically by Raft when
	// this is possible. If false, the configuration will be joint until the
	// application initiates the transition manually.
	AutoLeave bool
	// Learners is a set of IDs corresponding to the learners active in the
	// current configuration.
	//
	// Invariant: Learners and Voters does not intersect, i.e. if a peer is in
	// either half of the joint config, it can't be a learner; if it is a
	// learner it can't be in either half of the joint config. This invariant
	// simplifies the implementation since it allows peers to have clarity about
	// its current role without taking into account joint consensus.
	Learners map[uint64]struct{}
	// When we turn a voter into a learner during a joint consensus transition,
	// we cannot add the learner directly when entering the joint state. This is
	// because this would violate the invariant that the intersection of
	// voters and learners is empty. For example, assume a Voter is removed and
	// immediately re-added as a learner (or in other words, it is demoted):
	//
	// Initially, the configuration will be
	//
	//   voters:   {1 2 3}
	//   learners: {}
	//
	// and we want to demote 3. Entering the joint configuration, we naively get
	//
	//   voters:   {1 2} & {1 2 3}
	//   learners: {3}
	//
	// but this violates the invariant (3 is both voter and learner). Instead,
	// we get
	//
	//   voters:   {1 2} & {1 2 3}
	//   learners: {}
	//   next_learners: {3}
	//
	// Where 3 is now still purely a voter, but we are remembering the intention
	// to make it a learner upon transitioning into the final configuration:
	//
	//   voters:   {1 2}
	//   learners: {3}
	//   next_learners: {}
	//
	// Note that next_learners is not used while adding a learner that is not
	// also a voter in the joint config. In this case, the learner is added
	// right away when entering the joint configuration, so that it is caught up
	// as soon as possible.
	LearnersNext map[uint64]struct{}
}

func (c Config) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "voters=%s", c.Voters)
	if c.Learners != nil {
		fmt.Fprintf(&buf, " learners=%s", quorum.MajorityConfig(c.Learners).String())
	}
	if c.LearnersNext != nil {
		fmt.Fprintf(&buf, " learners_next=%s", quorum.MajorityConfig(c.LearnersNext).String())
	}
	if c.AutoLeave {
		fmt.Fprint(&buf, " autoleave")
	}
	return buf.String()
}

// Clone returns a copy of the Config that shares no memory with the original.
func (c *Config) Clone() Config {
	clone := func(m map[uint64]struct{}) map[uint64]struct{} {
		if m == nil {
			return nil
		}
		mm := make(map[uint64]struct{}, len(m))
		for k := range m {
			mm[k] = struct{}{}
		}
		return mm
	}
	return Config{
		Voters:       quorum.JointConfig{clone(c.Voters[0]), clone(c.Voters[1])},
		Learners:     clone(c.Learners),
		LearnersNext: clone(c.LearnersNext),
	}
}

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
type ProgressTracker struct {
	Config

	Progress ProgressMap

	// Votes records the ballots cast in the election tracked by ballotTerm.
	// A value of true means the voter granted its vote, false means it
	// rejected the candidate.
	Votes map[uint64]bool

	// ballotTerm is the election term the current Votes pertain to. It is
	// used to prevent delayed responses from an old term from rewriting the
	// current ballot. 0 means no election is in progress.
	ballotTerm uint64

	MaxInflight      int
	MaxInflightBytes uint64
}

// MakeProgressTracker initializes a ProgressTracker.
func MakeProgressTracker(maxInflight int, maxBytes uint64) ProgressTracker {
	p := ProgressTracker{
		MaxInflight:      maxInflight,
		MaxInflightBytes: maxBytes,
		Config: Config{
			Voters: quorum.JointConfig{
				quorum.MajorityConfig{},
				nil, // only populated when used
			},
			Learners:     nil, // only populated when used
			LearnersNext: nil, // only populated when used
		},
		Votes:    map[uint64]bool{},
		Progress: map[uint64]*Progress{},
	}
	return p
}

// ConfState returns a ConfState representing the active configuration.
func (p *ProgressTracker) ConfState() *pb.ConfState {
	return &pb.ConfState{
		Voters:         p.Voters[0].Slice(),
		VotersOutgoing: p.Voters[1].Slice(),
		Learners:       quorum.MajorityConfig(p.Learners).Slice(),
		LearnersNext:   quorum.MajorityConfig(p.LearnersNext).Slice(),
		AutoLeave:      new(p.AutoLeave),
	}
}

// IsSingleton returns true if (and only if) there is only one voting member
// (i.e. the leader) in the current configuration.
func (p *ProgressTracker) IsSingleton() bool {
	return len(p.Voters[0]) == 1 && len(p.Voters[1]) == 0
}

type matchAckIndexer map[uint64]*Progress

var _ quorum.AckedIndexer = matchAckIndexer(nil)

// AckedIndex implements IndexLookuper.
func (l matchAckIndexer) AckedIndex(id uint64) (quorum.Index, bool) {
	pr, ok := l[id]
	if !ok {
		return 0, false
	}
	return quorum.Index(pr.Match), true
}

// Committed returns the largest log index known to be committed based on what
// the voting members of the group have acknowledged.
func (p *ProgressTracker) Committed() uint64 {
	return uint64(p.Voters.CommittedIndex(matchAckIndexer(p.Progress)))
}

// Visit invokes the supplied closure for all tracked progresses in stable order.
func (p *ProgressTracker) Visit(f func(id uint64, pr *Progress)) {
	n := len(p.Progress)
	// We need to sort the IDs and don't want to allocate since this is hot code.
	// The optimization here mirrors that in `(MajorityConfig).CommittedIndex`,
	// see there for details.
	var sl [7]uint64
	var ids []uint64
	if len(sl) >= n {
		ids = sl[:n]
	} else {
		ids = make([]uint64, n)
	}
	for id := range p.Progress {
		n--
		ids[n] = id
	}
	slices.Sort(ids)
	for _, id := range ids {
		f(id, p.Progress[id])
	}
}

// QuorumActive returns true if the quorum is active from the view of the local
// raft state machine. Otherwise, it returns false.
func (p *ProgressTracker) QuorumActive() bool {
	votes := map[uint64]bool{}
	p.Visit(func(id uint64, pr *Progress) {
		if pr.IsLearner {
			return
		}
		votes[id] = pr.RecentActive
	})

	return p.Voters.VoteResult(votes) == quorum.VoteWon
}

// VoterNodes returns a sorted slice of voters.
func (p *ProgressTracker) VoterNodes() []uint64 {
	m := p.Voters.IDs()
	nodes := make([]uint64, 0, len(m))
	for id := range m {
		nodes = append(nodes, id)
	}
	slices.Sort(nodes)
	return nodes
}

// LearnerNodes returns a sorted slice of learners.
func (p *ProgressTracker) LearnerNodes() []uint64 {
	if len(p.Learners) == 0 {
		return nil
	}
	nodes := make([]uint64, 0, len(p.Learners))
	for id := range p.Learners {
		nodes = append(nodes, id)
	}
	slices.Sort(nodes)
	return nodes
}

// VoteState is the state of a single voter's ballot in a leader election.
type VoteState byte

const (
	// VoteNotResponded means the voter has not responded in the election yet.
	VoteNotResponded VoteState = iota
	// VoteGranted means the voter granted its vote to the candidate.
	VoteGranted
	// VoteRejected means the voter rejected the candidate.
	VoteRejected
)

// String returns a human-readable representation of the vote state.
func (s VoteState) String() string {
	switch s {
	case VoteGranted:
		return "granted"
	case VoteRejected:
		return "rejected"
	default:
		return "not-responded"
	}
}

// BallotVote is a single voter's ballot in a candidate election.
type BallotVote struct {
	// ID is the voter's node ID.
	ID uint64
	// State is the voter's response in the election.
	State VoteState
}

// BallotSummary is a point-in-time snapshot of the votes cast in a
// candidate's current election. Voters are listed in stable node-ID order so
// the summary is deterministic across repeated queries.
type BallotSummary struct {
	// Term is the election term the ballot pertains to. It is 0 when no
	// election is in progress (e.g. the node is a follower or a settled
	// leader).
	Term uint64
	// Votes is the ballot state of every voting member, sorted by node ID.
	Votes []BallotVote
}

// ResetVotes prepares for a new round of vote counting via recordVote. It
// clears all ballots and tags the round with the given term (the term the
// candidacy is polling for), so entering a new term always starts from an
// empty ballot.
func (p *ProgressTracker) ResetVotes(term uint64) {
	p.Votes = map[uint64]bool{}
	p.ballotTerm = term
}

// RecordVote records that the node with the given id voted for this Raft
// instance if v == true (and declined it otherwise). msgTerm is the term
// carried by the response. A response from a term older than the currently
// tracked election is ignored, so a delayed message from a previous
// candidacy cannot rewrite the current ballot. Responses carrying a term at
// least as new as the candidacy's term are accepted: for pre-vote, grants
// arrive one term ahead and rejections carry the responder's term, which may
// be older than the polled term but never older than the candidacy's term.
func (p *ProgressTracker) RecordVote(msgTerm, id uint64, v bool) {
	if p.ballotTerm != 0 && msgTerm < p.ballotTerm {
		// Stale response from an old term: leave the current ballot intact.
		return
	}
	_, ok := p.Votes[id]
	if !ok {
		p.Votes[id] = v
	}
}

// BallotSummary returns a sorted snapshot of the current election ballot.
// The summary is empty when no election is in progress (the node is a
// follower or a settled leader).
func (p *ProgressTracker) BallotSummary() BallotSummary {
	if len(p.Votes) == 0 {
		return BallotSummary{}
	}
	s := BallotSummary{Term: p.ballotTerm}
	ids := p.VoterNodes()
	s.Votes = make([]BallotVote, 0, len(ids))
	for _, id := range ids {
		state := VoteNotResponded
		if v, ok := p.Votes[id]; ok {
			if v {
				state = VoteGranted
			} else {
				state = VoteRejected
			}
		}
		s.Votes = append(s.Votes, BallotVote{ID: id, State: state})
	}
	return s
}

// TallyVotes returns the number of granted and rejected Votes, and whether the
// election outcome is known.
func (p *ProgressTracker) TallyVotes() (granted int, rejected int, _ quorum.VoteResult) {
	// Make sure to populate granted/rejected correctly even if the Votes slice
	// contains members no longer part of the configuration. This doesn't really
	// matter in the way the numbers are used (they're informational), but might
	// as well get it right.
	for id, pr := range p.Progress {
		if pr.IsLearner {
			continue
		}
		v, voted := p.Votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	result := p.Voters.VoteResult(p.Votes)
	return granted, rejected, result
}
