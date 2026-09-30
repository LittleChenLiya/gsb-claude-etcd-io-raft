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
)

// localTerm is a test helper that returns the local log term at i via
// raftLog.term, without panicking on an out-of-bounds index.
func localTerm(l *raftLog, i uint64) uint64 {
	t, err := l.term(i)
	return l.zeroTermOnOutOfBounds(t, err)
}

// TestLocateConflictTable checks the raw conflict-segment locator against the
// same cases as TestFindConflict, asserting additionally the term at the first
// conflicting index.
func TestLocateConflictTable(t *testing.T) {
	previousEnts := index(1).terms(1, 2, 3)
	tests := []struct {
		name      string
		ents      []*pb.Entry
		wantIndex uint64
		wantTerm  uint64 // local term at the conflict index, 0 if beyond lastIndex
		wantHit   bool
	}{
		{"empty", nil, 0, 0, false},
		{"same prefix, all contained", index(1).terms(1, 2, 3), 0, 0, false},
		{"prefix contained", index(2).terms(2, 3), 0, 0, false},
		{"single matching entry", index(3).terms(3), 0, 0, false},
		// Same prefix, extending tail: the first new index is reported.
		{"extends tail", index(1).terms(1, 2, 3, 4, 4), 4, 0, true},
		{"extends tail from 2", index(2).terms(2, 3, 4, 5), 4, 0, true},
		{"extends tail from 4", index(4).terms(4, 4), 4, 0, true},
		// Forked suffix: same index, different term.
		{"fork at 1", index(1).terms(4, 4), 1, 1, true},
		{"fork at 2", index(2).terms(1, 4, 4), 2, 2, true},
		{"fork at 3", index(3).terms(1, 2, 4, 4), 3, 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLog(NewMemoryStorage(), raftLogger)
			l.append(previousEnts...)

			cp, hit := l.locateConflict(tt.ents)
			require.Equal(t, tt.wantHit, hit)
			require.Equal(t, tt.wantIndex, cp.index)
			require.Equal(t, tt.wantTerm, cp.term)

			// The reported term must always equal the local term at the
			// reported index, and the index must be addressable without
			// panicking.
			if hit {
				require.Equal(t, localTerm(l, cp.index), cp.term)
			}
			// Compatibility: findConflict keeps its original result.
			require.Equal(t, tt.wantIndex, l.findConflict(tt.ents))
		})
	}
}

// TestLocateAppendConflictPaths exercises the complete append decision against
// in-memory logs: a matching prefix, a forked suffix, new (extending) entries,
// a prev-term mismatch rejection, and the empty-log origin.
func TestLocateAppendConflictPaths(t *testing.T) {
	// Local log: entries at 1..3 with terms 1,2,3; none committed so that a
	// forked suffix may legally truncate the tail.
	base := func(t *testing.T) *raftLog {
		l := newLog(NewMemoryStorage(), raftLogger)
		l.append(index(1).terms(1, 2, 3)...)
		require.Equal(t, uint64(3), l.lastIndex())
		require.Equal(t, uint64(0), l.committed)
		return l
	}

	t.Run("same prefix extends tail", func(t *testing.T) {
		l := base(t)
		// prev=(3,3) matches; entries 4,5 are new.
		a := logSlice{
			term:    100,
			prev:    entryID{term: 3, index: 3},
			entries: index(4).terms(4, 4),
		}
		cp, rejected := l.locateAppendConflict(a)
		require.False(t, rejected)
		require.Equal(t, uint64(4), cp.index)
		require.Equal(t, uint64(0), cp.term) // index 4 is beyond lastIndex

		// Original append semantics: succeeds, commits up to the requested
		// value, and does not move the commit past the new last index.
		lasti, ok := l.maybeAppend(a, 5)
		require.True(t, ok)
		require.Equal(t, uint64(5), lasti)
		require.Equal(t, uint64(5), l.committed)
		require.Equal(t, uint64(5), l.lastIndex())
	})

	t.Run("prefix subset fully contained", func(t *testing.T) {
		l := base(t)
		// prev=(1,1) matches, and entries 2,3 are identical.
		a := logSlice{
			term:    100,
			prev:    entryID{term: 1, index: 1},
			entries: index(2).terms(2, 3),
		}
		cp, rejected := l.locateAppendConflict(a)
		require.False(t, rejected)
		require.Equal(t, uint64(0), cp.index)
		require.Equal(t, uint64(0), cp.term)

		lasti, ok := l.maybeAppend(a, 3)
		require.True(t, ok)
		require.Equal(t, uint64(3), lasti)
		require.Equal(t, uint64(3), l.committed)
		// No tail rewrite.
		require.Equal(t, []uint64{1, 2, 3}, []uint64{
			localTerm(l, 1), localTerm(l, 2), localTerm(l, 3),
		})
	})

	t.Run("forked suffix truncates and rewrites", func(t *testing.T) {
		l := base(t)
		// prev=(2,2) matches; entry 3 arrives with term 4, forking the tail.
		a := logSlice{
			term:    100,
			prev:    entryID{term: 2, index: 2},
			entries: index(3).terms(4),
		}
		// The fork is an accepted-but-diverging append, not a rejection.
		cp, rejected := l.locateAppendConflict(a)
		require.False(t, rejected)
		require.Equal(t, uint64(3), cp.index)
		require.Equal(t, uint64(3), cp.term) // local term at 3

		// Original append semantics: truncate at 3 and rewrite; the commit
		// index before the fork (0) is moved only via commitTo.
		require.Equal(t, uint64(0), l.committed)
		lasti, ok := l.maybeAppend(a, 3)
		require.True(t, ok)
		require.Equal(t, uint64(3), lasti)
		require.Equal(t, uint64(3), l.committed)
		require.Equal(t, uint64(4), localTerm(l, 3))
		require.Equal(t, uint64(2), localTerm(l, 2)) // shared prefix preserved
	})

	t.Run("prev term mismatch rejects append", func(t *testing.T) {
		l := base(t)
		// prev=(3,9) does not exist locally; the append is rejected.
		a := logSlice{
			term:    100,
			prev:    entryID{term: 9, index: 3},
			entries: index(4).terms(9),
		}
		cp, rejected := l.locateAppendConflict(a)
		require.True(t, rejected)
		require.Equal(t, uint64(3), cp.index)
		require.Equal(t, uint64(3), cp.term) // local term at the mismatch

		// Original rejection semantics and unchanged commit index.
		lasti, ok := l.maybeAppend(a, 4)
		require.False(t, ok)
		require.Equal(t, uint64(0), lasti)
		require.Equal(t, uint64(0), l.committed)
		require.Equal(t, uint64(3), l.lastIndex())
	})

	t.Run("prev beyond last index rejects", func(t *testing.T) {
		l := base(t)
		// prev=(10,10) is beyond the local log: term unknown, reported as 0.
		a := logSlice{
			term:    100,
			prev:    entryID{term: 10, index: 10},
			entries: index(11).terms(10),
		}
		cp, rejected := l.locateAppendConflict(a)
		require.True(t, rejected)
		require.Equal(t, uint64(10), cp.index)
		require.Equal(t, uint64(0), cp.term)

		lasti, ok := l.maybeAppend(a, 11)
		require.False(t, ok)
		require.Equal(t, uint64(0), lasti)
		require.Equal(t, uint64(0), l.committed)
	})

	t.Run("empty log origin prev", func(t *testing.T) {
		l := newLog(NewMemoryStorage(), raftLogger)
		// Bootstrapping append: prev=(0,0) matches the empty log.
		a := logSlice{
			term:    1,
			prev:    entryID{term: 0, index: 0},
			entries: index(1).terms(1, 1),
		}
		cp, rejected := l.locateAppendConflict(a)
		require.False(t, rejected)
		require.Equal(t, uint64(1), cp.index)
		require.Equal(t, uint64(0), cp.term)

		lasti, ok := l.maybeAppend(a, 1)
		require.True(t, ok)
		require.Equal(t, uint64(2), lasti)
		require.Equal(t, uint64(1), l.committed)
	})
}

// TestLocateConflictSnapshotBoundary verifies that mismatches located in a
// compacted prefix are clamped to the snapshot boundary. The returned index
// must never precede firstIndex()-1, and its term must be the snapshot term.
func TestLocateConflictSnapshotBoundary(t *testing.T) {
	t.Run("compacted storage prefix", func(t *testing.T) {
		st := NewMemoryStorage()
		// Stable log 1..6, then compact through index 5. Storage retains the
		// boundary entry at (5,5); firstIndex() == 6.
		require.NoError(t, st.Append(index(1).terms(1, 2, 3, 4, 5, 6)))
		require.NoError(t, st.Compact(5))

		l := newLog(st, raftLogger)
		// newLog initializes commit to firstIndex()-1 == 5.
		require.Equal(t, uint64(6), l.firstIndex())
		require.Equal(t, uint64(5), l.committed)
		require.Equal(t, uint64(6), l.lastIndex())

		// A remote slice whose prev (3,9) lies in the compacted prefix and
		// mismatches. The conflict must clamp to the boundary index 5 with the
		// retained snapshot term 5, never returning an out-of-bounds index
		// such as 3.
		a := logSlice{
			term:    100,
			prev:    entryID{term: 9, index: 3},
			entries: index(4).terms(9),
		}
		cp, rejected := l.locateAppendConflict(a)
		require.True(t, rejected)
		require.Equal(t, uint64(5), cp.index)
		require.Equal(t, l.firstIndex()-1, cp.index)
		require.Equal(t, uint64(5), cp.term)
		// The reported index is addressable via term() without an error.
		require.Equal(t, uint64(5), mustTerm(l.term(cp.index)))

		// Original rejection semantics; commit index unchanged.
		lasti, ok := l.maybeAppend(a, 6)
		require.False(t, ok)
		require.Equal(t, uint64(0), lasti)
		require.Equal(t, uint64(5), l.committed)

		// Same guarantee via the entry-level locator: a mismatch at index 2
		// clamps to the boundary.
		cp, hit := l.locateConflict(index(2).terms(9))
		require.True(t, hit)
		require.Equal(t, l.firstIndex()-1, cp.index)
		require.Equal(t, uint64(5), cp.term)
	})

	t.Run("applied snapshot in unstable log", func(t *testing.T) {
		st := NewMemoryStorage()
		require.NoError(t, st.Append(index(1).terms(1, 2, 3)))
		l := newLog(st, raftLogger)
		// Restore from a snapshot at (10,7); firstIndex() becomes 11.
		snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: new(uint64(10)),
			Term:  new(uint64(7)),
		}}
		l.restore(snap)
		require.Equal(t, uint64(11), l.firstIndex())
		require.Equal(t, uint64(10), l.committed)

		// A prev mismatch at index 8, inside the snapshotted-away prefix, must
		// clamp to 10 and report the snapshot term 7.
		a := logSlice{
			term:    100,
			prev:    entryID{term: 3, index: 8},
			entries: index(9).terms(3),
		}
		cp, rejected := l.locateAppendConflict(a)
		require.True(t, rejected)
		require.Equal(t, uint64(10), cp.index)
		require.Equal(t, l.firstIndex()-1, cp.index)
		require.Equal(t, uint64(7), cp.term)
		require.Equal(t, uint64(7), mustTerm(l.term(cp.index)))

		// Commit index must remain at the snapshot index.
		_, ok := l.maybeAppend(a, 10)
		require.False(t, ok)
		require.Equal(t, uint64(10), l.committed)
	})

	t.Run("boundary term itself matches", func(t *testing.T) {
		st := NewMemoryStorage()
		require.NoError(t, st.Append(index(1).terms(1, 2, 3, 4, 5, 6)))
		require.NoError(t, st.Compact(5))
		l := newLog(st, raftLogger)

		// A prev at the boundary (5,5) matches, so the append is not rejected
		// even though everything below index 6 is compacted away.
		a := logSlice{
			term:    100,
			prev:    entryID{term: 5, index: 5},
			entries: index(6).terms(6, 8), // fork at 6 followed by 7
		}
		cp, rejected := l.locateAppendConflict(a)
		require.False(t, rejected)
		require.Equal(t, uint64(7), cp.index) // first new index after the fork
		require.Equal(t, uint64(0), cp.term)  // index 7 is beyond lastIndex

		lasti, ok := l.maybeAppend(a, 6)
		require.True(t, ok)
		require.Equal(t, uint64(7), lasti)
		require.Equal(t, uint64(6), l.committed)
		require.Equal(t, uint64(8), localTerm(l, 7))
	})
}
