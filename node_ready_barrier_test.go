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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3/raftpb"
)

// readyLeader is a single-node, single-peer, sync-storage Node that has been
// campaigned to leadership. It bundles the Node with its MemoryStorage so
// tests can persist Ready.Entries the way a real application would.
type readyLeader struct {
	n *node
	s *MemoryStorage
}

func newReadyLeader(t *testing.T) *readyLeader {
	t.Helper()
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	go n.run()
	t.Cleanup(n.Stop)
	l := &readyLeader{n: &n, s: s}

	require.NoError(t, l.n.Campaign(t.Context()))
	// Persist and acknowledge the vote and the leader's empty entry. Election
	// is observed through Ready.SoftState, which is safe to read off the
	// channel (the raft state itself is only touched by the run goroutine).
	leader := false
	for !leader {
		rd := l.drainReady(t)
		l.s.Append(rd.Entries)
		leader = rd.SoftState != nil && rd.SoftState.Lead == 1
		l.n.Advance()
	}
	// The empty leader entry arrives in one more Ready; drain it so tests
	// start from a clean fence.
	select {
	case rd := <-l.n.Ready():
		l.s.Append(rd.Entries)
		l.n.Advance()
	case <-time.After(time.Second):
	}
	return l
}

// drainReady returns the next Ready, failing the test if it does not arrive
// in time.
func (l *readyLeader) drainReady(t *testing.T) Ready {
	t.Helper()
	select {
	case rd := <-l.n.Ready():
		return rd
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Ready")
		return Ready{}
	}
}

// assertNoReady fails if a Ready is published within d.
func (l *readyLeader) assertNoReady(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case rd := <-l.n.Ready():
		t.Fatalf("Ready published while batch is unadvanced: %s", DescribeReady(rd, nil))
	case <-time.After(d):
	}
}

// entryData returns the payloads of the given entries.
func entryData(ents []*raftpb.Entry) [][]byte {
	var out [][]byte
	for _, e := range ents {
		out = append(out, e.GetData())
	}
	return out
}

// TestReadyBatchBarrier verifies the Ready batch-acknowledgment fence:
//   - the persisted entries of batch N are not published again before batch N
//     has been acknowledged with Advance (the fence blocks a second Ready);
//   - after Advance, batch N+1 is published and never repeats batch N's
//     persisted entries.
func TestReadyBatchBarrier(t *testing.T) {
	l := newReadyLeader(t)

	// Batch 1: propose two entries before consuming the Ready; they must be
	// published together and in order.
	require.NoError(t, l.n.Propose(t.Context(), []byte("a")))
	require.NoError(t, l.n.Propose(t.Context(), []byte("b")))

	batch1 := l.drainReady(t)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, entryData(batch1.Entries))
	// Persist them like the application's WAL would.
	l.s.Append(batch1.Entries)

	// Fence is closed: no further Ready may be published until Advance.
	l.assertNoReady(t, 50*time.Millisecond)

	// A new proposal while the fence is closed must not sneak batch 1's
	// persisted entries out again; it simply stays pending inside the Node.
	require.NoError(t, l.n.Propose(t.Context(), []byte("c")))
	l.assertNoReady(t, 50*time.Millisecond)

	// Acknowledge batch 1.
	l.n.Advance()

	// Batch 2 is now published. It must not repeat batch 1's entries.
	batch2 := l.drainReady(t)
	require.Equal(t, [][]byte{[]byte("c")}, entryData(batch2.Entries))
	l.s.Append(batch2.Entries)

	// Fence closes again for batch 2.
	l.assertNoReady(t, 50*time.Millisecond)
	l.n.Advance()

	// Persisted-entry indices must never be published twice.
	seen := map[uint64]bool{}
	for _, batch := range []Ready{batch1, batch2} {
		for _, e := range batch.Entries {
			assert.Falsef(t, seen[e.GetIndex()], "persisted entry index %d published twice", e.GetIndex())
			seen[e.GetIndex()] = true
		}
	}
}

// TestReadyBatchBarrierCancelRetainsBatch verifies that cancelling Ready
// consumers around an outstanding batch never loses the pending batch:
//
//  1. A waiter blocked on Ready with a cancellable context exits when its
//     context is cancelled without consuming a batch; a subsequent proposal is
//     still delivered exactly once, intact.
//  2. A consumer that received a batch, persisted it and then went away
//     without calling Advance leaves the fence closed (no silent republish,
//     the batch is neither swallowed nor duplicated): Stop does not block and,
//     after a restart over the same stable storage, the pending entry is
//     committed and applied again.
func TestReadyBatchBarrierCancelRetainsBatch(t *testing.T) {
	t.Run("cancelled blocked waiter keeps pending batch", func(t *testing.T) {
		l := newReadyLeader(t)

		// Waiter parks on Ready with a cancellable context while no batch is
		// armed yet.
		ctx, cancel := context.WithCancel(t.Context())
		got := make(chan Ready, 1)
		go func() {
			select {
			case rd := <-l.n.Ready():
				got <- rd
			case <-ctx.Done():
				got <- Ready{}
			}
		}()
		time.Sleep(20 * time.Millisecond)
		cancel()
		select {
		case rd := <-got:
			require.Empty(t, rd.Entries, "cancelled waiter must not receive a batch")
		case <-time.After(2 * time.Second):
			t.Fatal("cancelled waiter did not exit")
		}

		// Proposing after the waiter left still publishes the batch exactly
		// once to a subsequent consumer; nothing was dropped or duplicated.
		require.NoError(t, l.n.Propose(t.Context(), []byte("second")))
		rd := l.drainReady(t)
		require.Equal(t, [][]byte{[]byte("second")}, entryData(rd.Entries))
		l.s.Append(rd.Entries)
		l.n.Advance()

		// After acknowledgment, any follow-up Ready may carry the committed
		// entry for application (self-ack), but must never republish batch
		// entries that were already persisted.
		select {
		case rd := <-l.n.Ready():
			assert.Empty(t, rd.Entries, "persisted entries must not be republished after Advance")
			l.n.Advance()
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("cancelled after receive: batch survives without Advance", func(t *testing.T) {
		l := newReadyLeader(t)

		// A consumer receives a batch, durably persists it, then goes away
		// (cancelled) without calling Advance.
		require.NoError(t, l.n.Propose(t.Context(), []byte("pending")))
		rd := l.drainReady(t)
		require.Equal(t, [][]byte{[]byte("pending")}, entryData(rd.Entries))
		l.s.Append(rd.Entries)
		pendingIndex := rd.Entries[len(rd.Entries)-1].GetIndex()

		// New proposals while the old consumer is gone must not republish the
		// unacknowledged batch, nor get published over it: the fence stays
		// closed.
		require.NoError(t, l.n.Propose(t.Context(), []byte("after")))
		l.assertNoReady(t, 50*time.Millisecond)

		// Stopping with the batch unadvanced must not block and must not
		// discard the persisted batch.
		stopped := make(chan struct{})
		go func() {
			l.n.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Fatal("Stop blocked while a batch was unadvanced")
		}

		// Restart a Node over the same stable storage. Campaign again and
		// drive Readies; the unacknowledged entry must resurface and be
		// committed/applied, i.e. the pending batch was not lost.
		rn2 := newTestRawNode(1, 10, 1, l.s)
		n2 := newNode(rn2)
		go n2.run()
		require.NoError(t, n2.Campaign(t.Context()))
		deadline := time.After(2 * time.Second)
		found := false
		for !found {
			var rd2 Ready
			select {
			case rd2 = <-n2.Ready():
			case <-deadline:
				t.Fatalf("entry at index %d never reapplied after restart", pendingIndex)
			}
			l.s.Append(rd2.Entries)
			for _, e := range rd2.CommittedEntries {
				if e.GetIndex() == pendingIndex {
					assert.Equal(t, []byte("pending"), e.GetData())
					found = true
				}
			}
			n2.Advance()
		}
		n2.Stop()
	})
}

// TestReadyBatchBarrierStop verifies shutdown semantics:
//   - waiters blocked on Ready exit promptly after Stop (the channel is
//     closed);
//   - an accepted but unadvanced batch is not dropped (its data had been
//     persisted by the application);
//   - nothing blocks after Stop and Stop is idempotent.
func TestReadyBatchBarrierStop(t *testing.T) {
	l := newReadyLeader(t)

	// Publish and accept a batch, persist it, but do NOT Advance it.
	require.NoError(t, l.n.Propose(t.Context(), []byte("pending")))
	rd := l.drainReady(t)
	require.Equal(t, [][]byte{[]byte("pending")}, entryData(rd.Entries))
	// Application durably persisted the batch before the stop.
	l.s.Append(rd.Entries)
	lastIndex, err := l.s.LastIndex()
	require.NoError(t, err)

	// Now park a waiter. The fence is closed (batch unadvanced), so no Ready
	// is published while it waits; it must only be released by Stop closing
	// the channel.
	waiterExited := make(chan struct{})
	go func() {
		for range l.n.Ready() {
			// Drain until the channel is closed.
		}
		close(waiterExited)
	}()
	time.Sleep(20 * time.Millisecond) // let the waiter park

	// Stop must return promptly despite the unadvanced batch and must wake
	// the parked waiter.
	stopReturned := make(chan struct{})
	go func() {
		l.n.Stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked while a batch was unadvanced")
	}
	select {
	case <-waiterExited:
	case <-time.After(2 * time.Second):
		t.Fatal("Ready waiter did not exit after Stop")
	}

	// Stop is idempotent.
	l.n.Stop()

	// The unadvanced batch was not discarded: its entry is still in stable
	// storage and can be replayed by a fresh consumer.
	ents, err := l.s.Entries(lastIndex, lastIndex+1, 1)
	require.NoError(t, err)
	require.Len(t, ents, 1)
	require.Equal(t, []byte("pending"), ents[0].GetData())
}
