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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3/quorum"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// mkPreflightChanger returns a Changer tracking the given voters.
func mkPreflightChanger(voters ...uint64) Changer {
	trk := tracker.ProgressTracker{
		Config:   tracker.Config{Voters: quorum.JointConfig{quorum.MajorityConfig{}}},
		Progress: tracker.ProgressMap{},
	}
	for _, id := range voters {
		trk.Config.Voters[0][id] = struct{}{}
		trk.Progress[id] = &tracker.Progress{}
	}
	return Changer{Tracker: trk, LastIndex: 10}
}

func add(id uint64) *pb.ConfChangeSingle {
	return &pb.ConfChangeSingle{Type: pb.ConfChangeAddNode.Enum(), NodeId: &id}
}

func del(id uint64) *pb.ConfChangeSingle {
	return &pb.ConfChangeSingle{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: &id}
}

// voterDiff computes (added, removed) between two voter sets, sorted.
func voterDiff(before, after quorum.MajorityConfig) (added, removed []uint64) {
	for id := range after {
		if _, ok := before[id]; !ok {
			added = append(added, id)
		}
	}
	for id := range before {
		if _, ok := after[id]; !ok {
			removed = append(removed, id)
		}
	}
	sorted := func(s []uint64) []uint64 {
		for i := 1; i < len(s); i++ {
			for j := i; j > 0 && s[j] < s[j-1]; j-- {
				s[j], s[j-1] = s[j-1], s[j]
			}
		}
		return s
	}
	return sorted(added), sorted(removed)
}

// TestPreflight runs table-driven checks of the preflight report against the
// outcome of actually applying the same changes through the Changer.
func TestPreflight(t *testing.T) {
	tests := []struct {
		name    string
		voters  []uint64
		ccs     []*pb.ConfChangeSingle
		want    PreflightReport
		wantErr string
	}{
		{
			name:   "add voter",
			voters: []uint64{1, 2, 3},
			ccs:    []*pb.ConfChangeSingle{add(4)},
			want:   PreflightReport{AddedVoters: []uint64{4}},
		},
		{
			name:   "remove voter",
			voters: []uint64{1, 2, 3},
			ccs:    []*pb.ConfChangeSingle{del(2)},
			want:   PreflightReport{RemovedVoters: []uint64{2}},
		},
		{
			name:   "swap voter needs joint",
			voters: []uint64{1, 2, 3},
			ccs:    []*pb.ConfChangeSingle{del(1), add(4)},
			want: PreflightReport{
				AddedVoters:   []uint64{4},
				RemovedVoters: []uint64{1},
				NeedJoint:     true,
			},
		},
		{
			name:    "duplicate add",
			voters:  []uint64{1, 2, 3},
			ccs:     []*pb.ConfChangeSingle{add(4), add(4)},
			wantErr: "duplicate change for node 4",
		},
		{
			name:    "duplicate add and remove same node",
			voters:  []uint64{1, 2, 3},
			ccs:     []*pb.ConfChangeSingle{add(4), del(4)},
			wantErr: "duplicate change for node 4",
		},
		{
			name:    "zero node id",
			voters:  []uint64{1, 2, 3},
			ccs:     []*pb.ConfChangeSingle{add(0)},
			wantErr: "node ID must not be zero",
		},
		{
			name:    "remove last voter",
			voters:  []uint64{1},
			ccs:     []*pb.ConfChangeSingle{del(1)},
			wantErr: "cannot remove the last voter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := mkPreflightChanger(tt.voters...)
			cfgBefore := c.Tracker.Config.Clone()

			rep, err := c.Preflight(tt.ccs...)

			// Preflight must never modify the existing configuration.
			assert.Equal(t, cfgBefore, c.Tracker.Config, "preflight modified the config")

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, rep)

			// Apply the changes for real and compare the resulting voter-set
			// diff against the preflight report.
			before := quorum.MajorityConfig{}
			for id := range c.Tracker.Config.Voters[0] {
				before[id] = struct{}{}
			}
			var after quorum.MajorityConfig
			if rep.NeedJoint {
				cfg, trk, err := c.EnterJoint(false /* autoLeave */, tt.ccs...)
				require.NoError(t, err)
				c.Tracker.Config, c.Tracker.Progress = cfg, trk
				cfg, trk, err = c.LeaveJoint()
				require.NoError(t, err)
				after = cfg.Voters[0]
			} else {
				cfg, _, err := c.Simple(tt.ccs...)
				require.NoError(t, err)
				after = cfg.Voters[0]
			}
			added, removed := voterDiff(before, after)
			assert.Equal(t, rep.AddedVoters, added, "added voters: preflight vs actual")
			assert.Equal(t, rep.RemovedVoters, removed, "removed voters: preflight vs actual")
		})
	}
}
