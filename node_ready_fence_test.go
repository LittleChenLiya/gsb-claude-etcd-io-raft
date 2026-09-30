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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

// assertNoReady asserts that no Ready is published on n.Ready() within d.
// It is used to verify that the batch-confirmation fence is holding back the
// next batch while the previous one is still in flight.
func assertNoReady(t *testing.T, n Node, d time.Duration) {
	t.Helper()
	select {
	case rd := <-n.Ready():
		t.Fatalf("unexpected Ready published while fence is closed: %+v", rd)
	case <-time.After(d):
	}
}

// storageHas reports whether s contains a normal entry whose Data equals data.
func storageHas(s *MemoryStorage, data string) bool {
	lo, err := s.FirstIndex()
	if err != nil {
		return false
	}
	hi, err := s.LastIndex()
	if err != nil {
		return false
	}
	ents, err := s.Entries(lo, hi+1, math.MaxUint64)
	if err != nil {
		return false
	}
	for _, e := range ents {
		if e.GetType() == pb.EntryNormal && string(e.GetData()) == data {
			return true
		}
	}
	return false
}

// consumeUntilLeader consumes and Advances Readies until the node becomes
// leader. It returns once the node is the leader.
func consumeUntilLeader(t *testing.T, n *node, s *MemoryStorage) {
	t.Helper()
	r := n.rn.raft
	for {
		rd := readyWithTimeout(n)
		require.NoError(t, s.Append(rd.Entries))
		if rd.SoftState != nil && rd.SoftState.Lead == r.id {
			n.Advance()
			return
		}
		n.Advance()
	}
}

// drainReadies consumes and Advances every Ready that is currently pending on
// n.Ready(), returning once no more are immediately available.
func drainReadies(t *testing.T, n *node, s *MemoryStorage) {
	t.Helper()
	for {
		select {
		case rd := <-n.Ready():
			require.NoError(t, s.Append(rd.Entries))
			n.Advance()
		case <-time.After(100 * time.Millisecond):
			return
		}
	}
}

// TestNodeReadyBatchFence verifies the "Ready batch confirmation fence":
//
//  1. While a Ready batch is in flight (emitted but not yet Advance'd), the
//     next batch (and its persistence messages) MUST NOT be published.
//  2. Cancelling the consumer does not drop the in-flight batch: the fence is
//     not released by consumer cancellation, and the batch can still be
//     Advance'd afterwards.
//  3. After Stop, every waiter (Propose, Ready reader, ...) exits without
//     blocking.
//
// A barrier channel is used to gate the consumer's Advance between two
// batches, so the publish order can be observed deterministically.
func TestNodeReadyBatchFence(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	go n.run()
	defer n.Stop()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, n.Campaign(ctx))
	consumeUntilLeader(t, &n, s)

	// Drain any remaining Readies (e.g. the committed no-op entry) before the
	// consumer starts, so the barrier only gates the post-leader batches.
	drainReadies(t, &n, s)

	// barrier is the batch-confirmation barrier: the test sends one signal to
	// release the consumer's Advance of the current batch.
	barrier := make(chan struct{})
	consumerCancel := make(chan struct{})
	consumerDone := make(chan struct{})

	go func() {
		defer close(consumerDone)
		for {
			select {
			case <-consumerCancel:
				return
			case rd := <-n.Ready():
				require.NoError(t, s.Append(rd.Entries))
				// Hold the batch until the barrier releases it.
				select {
				case <-barrier:
				case <-consumerCancel:
					return
				}
				n.Advance()
			}
		}
	}()

	// --- Batch 1: propose and let the consumer persist it, but hold Advance
	//     on the barrier.
	require.NoError(t, n.Propose(ctx, []byte("batch-1")))
	require.Eventually(t, func() bool { return storageHas(s, "batch-1") },
		time.Second, 10*time.Millisecond, "batch 1 was not persisted")

	// Propose batch 2 while batch 1 is still in flight (the consumer is
	// waiting on the barrier and has not Advance'd batch 1).
	require.NoError(t, n.Propose(ctx, []byte("batch-2")))

	// The fence MUST prevent batch 2 from being published while batch 1 is in
	// flight.
	assertNoReady(t, &n, 100*time.Millisecond)
	assert.False(t, storageHas(s, "batch-2"), "batch 2 was published before batch 1 was Advance'd")

	// Release the barrier -> the consumer Advances batch 1 -> the fence opens
	// -> batch 2 is published.
	barrier <- struct{}{}
	require.Eventually(t, func() bool { return storageHas(s, "batch-2") },
		time.Second, 10*time.Millisecond, "batch 2 was not published after batch 1 was Advance'd")

	// Release batch 2's Advance as well, so the fence is open for batch 3.
	barrier <- struct{}{}

	// --- Batch 3: cancel the consumer while a batch is in flight.
	require.NoError(t, n.Propose(ctx, []byte("batch-3")))
	require.Eventually(t, func() bool { return storageHas(s, "batch-3") },
		time.Second, 10*time.Millisecond, "batch 3 was not persisted")

	// Cancel the consumer. The in-flight batch 3 MUST NOT be lost: the fence
	// is not released by consumer cancellation.
	close(consumerCancel)
	<-consumerDone

	// The fence is still armed (batch 3 retained, not lost): no new Ready is
	// published.
	assertNoReady(t, &n, 100*time.Millisecond)

	// The retained batch can still be Advance'd after cancellation.
	n.Advance()

	// --- Stop must unblock all waiters.
	n.Stop()

	// A Propose after Stop must return ErrStopped instead of blocking.
	err := n.Propose(ctx, []byte("after-stop"))
	assert.Equal(t, ErrStopped, err)
}

// TestNodeReadyBatchFenceStopUnblocksReader verifies that after Stop, a
// goroutine blocked reading from Ready() exits instead of leaking.
func TestNodeReadyBatchFenceStopUnblocksReader(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, s)
	n := newNode(rn)
	go n.run()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, n.Campaign(ctx))
	consumeUntilLeader(t, &n, s)

	// A goroutine blocked reading Ready. It should exit after Stop.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case _, ok := <-n.Ready():
				if !ok {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Give the reader time to block on Ready.
	time.Sleep(20 * time.Millisecond)
	n.Stop()

	select {
	case <-readerDone:
	case <-time.After(time.Second):
		t.Fatal("Ready reader did not exit after Stop")
	}
}
