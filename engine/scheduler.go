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

package engine

import (
	"fmt"
	"sort"

	"github.com/coreos/fleet/agent"
	"github.com/coreos/fleet/job"
)

type decision struct {
	machineID string
}

type Scheduler interface {
	Decide(*clusterState, *job.Job) (*decision, error)
}

const (
	WeightNumUnits         = 0.5
	WeightCPUload          = 0.3
	WeightMemUsage         = 0.1
	WeightDockerContainers = 0.1
)

type leastLoadedScheduler struct{}

func (lls *leastLoadedScheduler) Decide(clust *clusterState, j *job.Job) (*decision, error) {
	agents := lls.sortedAgents(clust)

	if len(agents) == 0 {
		return nil, fmt.Errorf("zero agents available")
	}

	var target *agent.AgentState
	for _, as := range agents {
		if able, _ := as.AbleToRun(j); !able {
			continue
		}

		as := as
		target = as
		break
	}

	if target == nil {
		return nil, fmt.Errorf("no agents able to run job")
	}

	dec := decision{
		machineID: target.MState.ID,
	}

	return &dec, nil
}

// sortedAgents returns a list of AgentState objects sorted ascending
// by the number of scheduled units
func (lls *leastLoadedScheduler) sortedAgents(clust *clusterState) []*agent.AgentState {
	agents := clust.agents()

	sas := make(sortableAgentStates, 0)
	for _, as := range agents {
		sas = append(sas, as)
	}
	sort.Sort(sas)

	return []*agent.AgentState(sas)
}

type sortableAgentStates []*agent.AgentState

func (sas sortableAgentStates) Len() int      { return len(sas) }
func (sas sortableAgentStates) Swap(i, j int) { sas[i], sas[j] = sas[j], sas[i] }

func (sas sortableAgentStates) Less(i, j int) bool {
	iWeight, _ := calculateAgentStatsWeight(sas[i])
	jWeight, _ := calculateAgentStatsWeight(sas[j])
	fmt.Println("Weight calculation: %s sas[i].MState.ID weight: %f", sas[i].MState.ID, iWeight)
	fmt.Println("Weight calculation: %s sas[j].MState.ID weight: %f", sas[j].MState.ID, jWeight)
	return iWeight < jWeight
}

func calculateAgentStatsWeight(agent *agent.AgentState) (float64, error) {
	agentLoad, err := agent.Stats()
	if err != nil {
		return 0, err
	}

	return (WeightNumUnits*float64(len(agent.Units)) + WeightCPUload*agentLoad.Load1 + WeightMemUsage*(float64(agentLoad.MemPercent/100)) + WeightDockerContainers*float64(agentLoad.NumDockerContainers)), nil
}
