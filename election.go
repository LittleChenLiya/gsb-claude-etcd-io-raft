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
	"cmp"
	"slices"

	pb "go.etcd.io/raft/v3/raftpb"
)

// BallotOutcome is the state of a single peer's vote in the current
// (pre-)election round.
type BallotOutcome int

const (
	// BallotPending means that no response (granted or rejected) has been
	// received from this peer in the current election round.
	BallotPending BallotOutcome = iota
	// BallotGranted means that the peer granted its vote.
	BallotGranted
	// BallotRejected means that the peer rejected the vote.
	BallotRejected
)

func (o BallotOutcome) String() string {
	switch o {
	case BallotPending:
		return "Pending"
	case BallotGranted:
		return "Granted"
	case BallotRejected:
		return "Rejected"
	default:
		return "Unknown"
	}
}

// Ballot describes one peer's vote in an election round.
type Ballot struct {
	// ID is the peer that cast (or has not yet cast) the vote.
	ID uint64
	// Term is the term carried by the vote response. It equals
	// BallotSummary.Term for all responses that were counted in this round.
	Term uint64
	// Outcome is the peer's vote outcome.
	Outcome BallotOutcome
}

// BallotSummary is a queryable point-in-time summary of the ballots of the
// current (pre-)election round.
//
// Ballots contains one entry per current voter, sorted by peer ID. Peers that
// have not responded are reported as BallotPending. The summary is reset
// (empty) whenever the node enters a new term, and it is initialized when the
// node starts a (pre-)campaign. A response from an older term never updates
// the summary.
type BallotSummary struct {
	// Term is the term of the election being tracked. It is 0 when there is
	// no election round to report (e.g. the node is a follower, or a new term
	// has just begun).
	Term uint64
	// Ballots is the per-peer vote list, sorted by ID.
	Ballots []Ballot
}

// Empty reports whether there is no tracked election round.
func (s BallotSummary) Empty() bool { return s.Term == 0 && len(s.Ballots) == 0 }

func (s BallotSummary) filter(o BallotOutcome) []uint64 {
	var ids []uint64
	for _, b := range s.Ballots {
		if b.Outcome == o {
			ids = append(ids, b.ID)
		}
	}
	return ids
}

// Granted returns the IDs (sorted) of peers that granted their vote.
func (s BallotSummary) Granted() []uint64 { return s.filter(BallotGranted) }

// Rejected returns the IDs (sorted) of peers that rejected the vote.
func (s BallotSummary) Rejected() []uint64 { return s.filter(BallotRejected) }

// Pending returns the IDs (sorted) of peers that have not responded yet.
func (s BallotSummary) Pending() []uint64 { return s.filter(BallotPending) }

// resetBallots starts a fresh ballot summary for an election at the given
// term. Every current voter is initialized with BallotPending.
func (r *raft) resetBallots(term uint64) {
	r.ballotTerm = term
	r.ballots = make(map[uint64]BallotOutcome, len(r.trk.Voters.IDs()))
	for id := range r.trk.Voters.IDs() {
		r.ballots[id] = BallotPending
	}
}

// recordBallot records a vote response from id for the current election
// round. Responses that do not belong to the tracked round (in particular
// delayed responses from an older term) are ignored and never overwrite the
// current summary. It returns false when the response was not counted.
func (r *raft) recordBallot(id uint64, m *pb.Message) bool {
	if r.ballots == nil || r.ballotTerm == 0 {
		return false
	}
	if _, ok := r.ballots[id]; !ok {
		return false
	}
	// Responses belonging to this round carry either the tracked term
	// (real-vote responses, and granted PreVote responses), or -- for a
	// PreVote rejection -- the node's current term, since PreVote does not
	// bump it. Anything else (e.g. a delayed response from an older term) is
	// stale and must not rewrite the current summary.
	curTerm := r.ballotTerm == m.GetTerm()
	stalePreVoteReject := r.state == StatePreCandidate &&
		m.GetType() == pb.MsgPreVoteResp && m.GetReject() && m.GetTerm() == r.Term
	if !curTerm && !stalePreVoteReject {
		return false
	}
	if m.GetReject() {
		r.ballots[id] = BallotRejected
	} else {
		r.ballots[id] = BallotGranted
	}
	return true
}

// ballotSummary returns a snapshot of the current election round's ballots.
// The returned ballots are stably sorted by peer ID.
func (r *raft) ballotSummary() BallotSummary {
	if r.ballotTerm == 0 || len(r.ballots) == 0 {
		return BallotSummary{Term: r.ballotTerm}
	}
	s := BallotSummary{Term: r.ballotTerm, Ballots: make([]Ballot, 0, len(r.ballots))}
	for id, outcome := range r.ballots {
		s.Ballots = append(s.Ballots, Ballot{ID: id, Term: r.ballotTerm, Outcome: outcome})
	}
	slices.SortFunc(s.Ballots, func(a, b Ballot) int { return cmp.Compare(a.ID, b.ID) })
	return s
}
