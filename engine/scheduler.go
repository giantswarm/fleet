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
	Decide(*clusterState, ...*job.Job) (*decision, error)
}

type leastLoadedScheduler struct{}

func (lls *leastLoadedScheduler) Decide(clust *clusterState, jobs ...*job.Job) (*decision, error) {
	agents := lls.sortedAgents(clust)

	if len(agents) == 0 {
		return nil, fmt.Errorf("zero agents available")
	}

	var target *agent.AgentState
	for _, as := range agents {
		if able, _ := as.AbleToRun(jobs...); !able {
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
	niUnits := len(sas[i].Units)
	njUnits := len(sas[j].Units)
	return niUnits < njUnits || (niUnits == njUnits && sas[i].MState.ID < sas[j].MState.ID)
}

type jobsList []*job.Job

func (jobslist jobsList) GroupByPeers() [][]string {
	deps := map[string]*job.Job{}
	for _, j := range jobslist {
		deps[j.Name] = j
	}

	toVisit := map[string]struct{}{}
	for k, _ := range deps {
		toVisit[k] = struct{}{}
	}

	popOne := func() string {
		for k, _ := range toVisit {
			delete(toVisit, k)
			return k
		}
		return ""
	}

	groups := [][]string{}

	for {
		if len(toVisit) == 0 {
			break
		}

		lead := deps[popOne()]

		currentGroup := []string{lead.Name}

		inCurrentGroup := func(n string) bool {
			for _, v := range currentGroup {
				if v == n {
					return true
				}
			}
			return false
		}

		depsToCheck := lead.Peers()
		for {
			if len(depsToCheck) == 0 {
				break
			}
			d := depsToCheck[0]
			depsToCheck = depsToCheck[1:]

			if _, exists := deps[d]; exists {
				for _, deepDep := range deps[d].Peers() {
					if !inCurrentGroup(deepDep) {
						depsToCheck = append(depsToCheck, deepDep)
					}
				}

			}

			currentGroup = append(currentGroup, d)
			delete(toVisit, d)
		}
		groups = append(groups, currentGroup)
	}

	return groups
}
