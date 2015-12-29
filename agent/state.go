// Copyright 2014 CoreOS, Inc.
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

package agent

import (
	"fmt"
	"path"
	"time"

	"github.com/coreos/fleet/job"
	"github.com/coreos/fleet/log"
	"github.com/coreos/fleet/machine"

	"github.com/shirou/gopsutil/docker"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
)

type AgentState struct {
	MState              *machine.MachineState
	Units               map[string]*job.Unit
	SystemResourceUsage *SystemLoad
}

type SystemLoad struct {
	Load1               float64
	Load5               float64
	Load15              float64
	MemPercent          float64
	MemFree             uint64
	NumDockerContainers int
}

var lastCollectionTimestamp = time.Now()

func NewAgentState(ms *machine.MachineState) *AgentState {
	return &AgentState{
		MState: ms,
		Units:  make(map[string]*job.Unit),
	}
}

func (as *AgentState) unitScheduled(name string) bool {
	return as.Units[name] != nil
}

// hasConflict determines whether there are any known conflicts with the given Unit
func (as *AgentState) hasConflict(pUnitName string, pConflicts []string) (found bool, conflict string) {
	for _, eUnit := range as.Units {
		if pUnitName == eUnit.Name {
			continue
		}

		for _, pConflict := range pConflicts {
			if globMatches(pConflict, eUnit.Name) {
				found = true
				conflict = eUnit.Name
				return
			}
		}

		for _, eConflict := range eUnit.Conflicts() {
			if globMatches(eConflict, pUnitName) {
				found = true
				conflict = eUnit.Name
				return
			}
		}
	}

	return
}

func globMatches(pattern, target string) bool {
	matched, err := path.Match(pattern, target)
	if err != nil {
		log.Debugf("Received error while matching pattern '%s': %v", pattern, err)
	}
	return matched
}

// AbleToRun determines if an Agent can run the provided Job based on
// the Agent's current state. A boolean indicating whether this is the
// case or not is returned. The following criteria is used:
//   - Agent must meet the Job's machine target requirement (if any)
//   - Agent must have all of the Job's required metadata (if any)
//   - Agent must have all required Peers of the Job scheduled locally (if any)
//   - Job must not conflict with any other Units scheduled to the agent
func (as *AgentState) AbleToRun(j *job.Job) (bool, string) {
	if tgt, ok := j.RequiredTarget(); ok && !as.MState.MatchID(tgt) {
		return false, fmt.Sprintf("agent ID %q does not match required %q", as.MState.ID, tgt)
	}

	metadata := j.RequiredTargetMetadata()
	if len(metadata) != 0 {
		if !machine.HasMetadata(as.MState, metadata) {
			return false, "local Machine metadata insufficient"
		}
	}

	peers := j.Peers()
	if len(peers) != 0 {
		for _, peer := range peers {
			if !as.unitScheduled(peer) {
				return false, fmt.Sprintf("required peer Unit(%s) is not scheduled locally", peer)
			}
		}
	}

	if cExists, cJobName := as.hasConflict(j.Name, j.Conflicts()); cExists {
		return false, fmt.Sprintf("found conflict with locally-scheduled Unit(%s)", cJobName)
	}

	return true, ""
}

func (as *AgentState) Stats() (*SystemLoad, error) {
	timeNow := time.Now()

	// Greater than 3minutes
	if timeNow.Sub(lastCollectionTimestamp) > 180 {
		v, err := mem.VirtualMemory()
		if err != nil {
			return nil, err
		}

		l, err := load.LoadAvg()
		if err != nil {
			return nil, err
		}

		dockerIDs, err := docker.GetDockerIDList()
		if err != nil {
			return nil, err
		}

		// almost every return value is a struct
		fmt.Printf("Machine ID: %s\n", as.MState.ID)
		fmt.Printf("Total: %v, Free:%v, UsedPercent:%f%%\n", v.Total, v.Free, v.UsedPercent)
		fmt.Printf("Load1: %f; Load5: %f; Load15: %f \n", l.Load1, l.Load5, l.Load15)
		fmt.Printf("NumDockerContainers: %d \n", len(dockerIDs))

		as.SystemResourceUsage = &SystemLoad{
			Load1:               l.Load1,
			Load5:               l.Load5,
			Load15:              l.Load15,
			MemPercent:          v.UsedPercent,
			MemFree:             v.Free,
			NumDockerContainers: len(dockerIDs),
		}

		// Store last time we collected the resource usage metrics
		lastCollectionTimestamp = timeNow

		return as.SystemResourceUsage, nil

	}

	return as.SystemResourceUsage, nil

}
