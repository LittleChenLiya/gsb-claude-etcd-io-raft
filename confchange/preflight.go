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
	"errors"
	"fmt"
	"slices"

	pb "go.etcd.io/raft/v3/raftpb"
)

// PreflightReport summarizes the effect a batch of configuration changes
// would have on the active configuration, without applying them. It allows
// callers to inspect the delta between the old and new voter sets and to
// learn whether the changes require a joint consensus transition before
// committing to them.
type PreflightReport struct {
	// AddedVoters is the sorted list of IDs that would join the incoming
	// majority config Voters[0] as voters.
	AddedVoters []uint64
	// RemovedVoters is the sorted list of IDs that would leave the incoming
	// majority config Voters[0] (via removal or demotion to learner).
	RemovedVoters []uint64
	// NeedJoint is true if the changes mutate the voter set by more than one
	// member, in which case they cannot be carried out as a simple (one-at-a-
	// time) change and require a joint consensus transition instead.
	NeedJoint bool
}

// Preflight validates the supplied configuration changes against the current
// configuration and reports how they would alter the voter set, without
// modifying the Changer's configuration or progress map in any way.
//
// It returns a definite error if any change carries a zero NodeID, if the
// same node is referenced by more than one change in the batch, or if the
// changes would remove the last voter from a non-empty configuration.
func (c Changer) Preflight(ccs ...*pb.ConfChangeSingle) (PreflightReport, error) {
	// Simulate the voter set on a copy; the tracker's config is only read.
	oldVoters := incoming(c.Tracker.Voters)
	newVoters := make(map[uint64]struct{}, len(oldVoters))
	for id := range oldVoters {
		newVoters[id] = struct{}{}
	}

	seen := make(map[uint64]struct{}, len(ccs))
	for _, cc := range ccs {
		id := cc.GetNodeId()
		if id == 0 {
			return PreflightReport{}, errors.New("preflight: node ID must not be zero")
		}
		if _, dup := seen[id]; dup {
			return PreflightReport{}, fmt.Errorf("preflight: duplicate change for node %d", id)
		}
		seen[id] = struct{}{}

		switch cc.GetType() {
		case pb.ConfChangeAddNode:
			newVoters[id] = struct{}{}
		case pb.ConfChangeAddLearnerNode:
			// Adding a voter as a learner demotes it, removing it from the
			// voter set; adding a brand-new learner does not touch it.
			delete(newVoters, id)
		case pb.ConfChangeRemoveNode:
			delete(newVoters, id)
		case pb.ConfChangeUpdateNode:
		default:
			return PreflightReport{}, fmt.Errorf("preflight: unexpected conf type %d", cc.GetType())
		}
	}

	if len(oldVoters) > 0 && len(newVoters) == 0 {
		return PreflightReport{}, errors.New("preflight: cannot remove the last voter")
	}

	var rep PreflightReport
	for id := range newVoters {
		if _, ok := oldVoters[id]; !ok {
			rep.AddedVoters = append(rep.AddedVoters, id)
		}
	}
	for id := range oldVoters {
		if _, ok := newVoters[id]; !ok {
			rep.RemovedVoters = append(rep.RemovedVoters, id)
		}
	}
	slices.Sort(rep.AddedVoters)
	slices.Sort(rep.RemovedVoters)
	rep.NeedJoint = symdiff(oldVoters, newVoters) > 1
	return rep, nil
}
