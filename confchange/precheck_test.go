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

package confchange

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func addNode(id uint64) *pb.ConfChangeSingle {
	return &pb.ConfChangeSingle{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(id)}
}

func removeNode(id uint64) *pb.ConfChangeSingle {
	return &pb.ConfChangeSingle{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(id)}
}

func voterSet(ids ...uint64) map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

// TestPrecheck runs a table-driven test over membership changes covering add,
// remove, duplicate and single-voter removal. For every valid proposal it
// compares the precheck report against the actual change result; for every
// invalid proposal it asserts a deterministic error and that the active
// configuration is left untouched.
func TestPrecheck(t *testing.T) {
	tests := []struct {
		name      string
		setup     []*pb.ConfChangeSingle // applied one-at-a-time via Simple
		changes   []*pb.ConfChangeSingle // the proposal under test
		wantErr   string                 // expected error substring; empty => valid
		wantOld   map[uint64]struct{}
		wantNew   map[uint64]struct{}
		wantJoint bool
	}{
		{
			name:    "add voter",
			setup:   []*pb.ConfChangeSingle{addNode(1)},
			changes: []*pb.ConfChangeSingle{addNode(2)},
			wantOld: voterSet(1),
			wantNew: voterSet(1, 2),
		},
		{
			name:    "remove voter",
			setup:   []*pb.ConfChangeSingle{addNode(1), addNode(2)},
			changes: []*pb.ConfChangeSingle{removeNode(2)},
			wantOld: voterSet(1, 2),
			wantNew: voterSet(1),
		},
		{
			name:      "add two voters requires joint",
			setup:     []*pb.ConfChangeSingle{addNode(1)},
			changes:   []*pb.ConfChangeSingle{addNode(2), addNode(3)},
			wantOld:   voterSet(1),
			wantNew:   voterSet(1, 2, 3),
			wantJoint: true,
		},
		{
			name:      "add and remove requires joint",
			setup:     []*pb.ConfChangeSingle{addNode(1), addNode(2)},
			changes:   []*pb.ConfChangeSingle{removeNode(2), addNode(3)},
			wantOld:   voterSet(1, 2),
			wantNew:   voterSet(1, 3),
			wantJoint: true,
		},
		{
			name:    "duplicate member",
			setup:   []*pb.ConfChangeSingle{addNode(1)},
			changes: []*pb.ConfChangeSingle{addNode(2), addNode(2)},
			wantErr: "duplicate member",
		},
		{
			name:    "zero node id",
			setup:   []*pb.ConfChangeSingle{addNode(1)},
			changes: []*pb.ConfChangeSingle{addNode(2), {Type: pb.ConfChangeAddNode.Enum(), NodeId: new(uint64(0))}},
			wantErr: "zero node id",
		},
		{
			name:    "remove last voter",
			setup:   []*pb.ConfChangeSingle{addNode(1)},
			changes: []*pb.ConfChangeSingle{removeNode(1)},
			wantErr: "removed all voters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := tracker.MakeProgressTracker(10, 0)
			c := Changer{Tracker: tr, LastIndex: 10}

			// Build the initial configuration one change at a time so that
			// Simple never sees more than one voter change per call.
			for _, cc := range tt.setup {
				cfg, trk, err := c.Simple(cc)
				require.NoError(t, err)
				c.Tracker.Config = cfg
				c.Tracker.Progress = trk
			}
			before := c.Tracker.Config.Clone()

			report, err := c.Precheck(tt.changes...)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				// A failed precheck must not modify the active configuration.
				require.Equal(t, before, c.Tracker.Config)

				// The last-voter removal is also refused by the actual change
				// path (Simple), deterministically and without side effects.
				if tt.name == "remove last voter" {
					_, _, serr := c.Simple(tt.changes...)
					require.ErrorContains(t, serr, "removed all voters")
					require.Equal(t, before, c.Tracker.Config)
				}
				return
			}
			require.NoError(t, err)

			// The precheck must not modify the active configuration either.
			require.Equal(t, before, c.Tracker.Config)

			require.True(t, reflect.DeepEqual(tt.wantOld, report.OldVoters),
				"old voters: want %v, got %v", tt.wantOld, report.OldVoters)
			require.True(t, reflect.DeepEqual(tt.wantNew, report.NewVoters),
				"new voters: want %v, got %v", tt.wantNew, report.NewVoters)
			require.Equal(t, tt.wantJoint, report.JointRequired)

			// Derived added/removed sets must agree with old/new.
			require.True(t, reflect.DeepEqual(
				diffVoters(report.NewVoters, report.OldVoters), report.AddedVoters),
				"added voters mismatch")
			require.True(t, reflect.DeepEqual(
				diffVoters(report.OldVoters, report.NewVoters), report.RemovedVoters),
				"removed voters mismatch")

			// Now carry out the actual change and compare its result with the
			// precheck report.
			if report.JointRequired {
				cfg, trk, err := c.EnterJoint(false /* autoLeave */, tt.changes...)
				require.NoError(t, err)
				c.Tracker.Config = cfg
				c.Tracker.Progress = trk
				cfg, trk, err = c.LeaveJoint()
				require.NoError(t, err)
				c.Tracker.Config = cfg
				c.Tracker.Progress = trk
			} else {
				cfg, trk, err := c.Simple(tt.changes...)
				require.NoError(t, err)
				c.Tracker.Config = cfg
				c.Tracker.Progress = trk
			}
			actual := map[uint64]struct{}(incoming(c.Tracker.Voters))
			require.True(t, reflect.DeepEqual(tt.wantNew, actual),
				"actual voters: want %v, got %v", tt.wantNew, actual)
		})
	}
}
