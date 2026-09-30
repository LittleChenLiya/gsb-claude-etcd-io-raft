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

package tracker

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3/quorum"
)

// TestBallotSummary checks that the ballot summary lists every voter in
// stable node-ID order with its response (granted, rejected or
// not-responded), regardless of the order in which votes were recorded.
func TestBallotSummary(t *testing.T) {
	p := MakeProgressTracker(256, 0)
	p.Voters[0] = quorum.MajorityConfig{1: {}, 2: {}, 3: {}}
	p.Progress = ProgressMap{1: {}, 2: {}, 3: {}}

	// No election in progress: the summary is empty.
	s := p.BallotSummary()
	require.Equal(t, uint64(0), s.Term)
	require.Empty(t, s.Votes)

	// Record votes out of order; the summary must still be sorted by ID.
	p.ResetVotes(2)
	p.RecordVote(2, 3, false) // rejection from 3
	p.RecordVote(2, 1, true)  // self vote from 1
	s = p.BallotSummary()
	require.Equal(t, uint64(2), s.Term)
	require.Equal(t, []BallotVote{
		{ID: 1, State: VoteGranted},
		{ID: 2, State: VoteNotResponded},
		{ID: 3, State: VoteRejected},
	}, s.Votes)

	// A repeated recording for the same voter does not flip the ballot.
	p.RecordVote(2, 3, true)
	s = p.BallotSummary()
	require.Equal(t, VoteRejected, s.Votes[2].State)
}

// TestBallotStaleTermGuard checks that vote responses from an old term
// cannot rewrite the current ballot, and that entering a new term (via
// ResetVotes) starts from an empty ballot.
func TestBallotStaleTermGuard(t *testing.T) {
	p := MakeProgressTracker(256, 0)
	p.Voters[0] = quorum.MajorityConfig{1: {}, 2: {}, 3: {}}
	p.Progress = ProgressMap{1: {}, 2: {}, 3: {}}

	// Current election is at term 2.
	p.ResetVotes(2)
	p.RecordVote(2, 1, true)
	p.RecordVote(2, 3, false)

	// Delayed responses from the previous (term 1) election arrive: they
	// must not rewrite the term-2 summary.
	p.RecordVote(1, 2, true)
	p.RecordVote(1, 3, true)
	// A response carrying term 0 is stale as well.
	p.RecordVote(0, 2, false)

	s := p.BallotSummary()
	require.Equal(t, uint64(2), s.Term)
	require.Equal(t, []BallotVote{
		{ID: 1, State: VoteGranted},
		{ID: 2, State: VoteNotResponded},
		{ID: 3, State: VoteRejected},
	}, s.Votes)

	// Entering a new term clears the old votes; the first vote of the new
	// term anchors the summary to that term.
	p.ResetVotes(3)
	s = p.BallotSummary()
	require.Equal(t, uint64(0), s.Term)
	require.Empty(t, s.Votes)

	p.RecordVote(3, 1, true)
	s = p.BallotSummary()
	require.Equal(t, uint64(3), s.Term)
	require.Equal(t, []BallotVote{
		{ID: 1, State: VoteGranted},
		{ID: 2, State: VoteNotResponded},
		{ID: 3, State: VoteNotResponded},
	}, s.Votes)

	// A pre-vote candidacy polls one term ahead: grants arrive at term 4 and
	// rejections arrive at the responder's term (3). Both must be accepted,
	// since neither is older than the candidacy's term.
	p.ResetVotes(3)
	p.RecordVote(4, 1, true)  // pre-vote grant at the polled term
	p.RecordVote(3, 2, false) // rejection from a responder at the candidacy's term
	s = p.BallotSummary()
	require.Equal(t, uint64(3), s.Term)
	require.Equal(t, VoteGranted, s.Votes[0].State)
	require.Equal(t, VoteRejected, s.Votes[1].State)
	require.Equal(t, VoteNotResponded, s.Votes[2].State)
}
