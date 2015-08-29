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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/fleet/job"
	"github.com/coreos/fleet/log"
	"github.com/coreos/fleet/machine"
	"github.com/coreos/fleet/pkg"
	"github.com/coreos/fleet/registry"
	"github.com/coreos/fleet/udplogger"
	"github.com/coreos/fleet/unit"
)

const (
	// TTL to use with all state pushed to Registry
	DefaultTTL = "30s"
)

type Agent struct {
	registry registry.Registry
	um       unit.UnitManager
	uGen     *unit.UnitStateGenerator
	Machine  machine.Machine
	ttl      time.Duration

	unitFileMux    map[string]*refCountedMux
	unitFileMuxMux *sync.Mutex

	cache *agentCache
}

type refCountedMux struct {
	mu       *sync.Mutex
	refcount int
}

func New(mgr unit.UnitManager, uGen *unit.UnitStateGenerator, reg registry.Registry, mach machine.Machine, ttl time.Duration) *Agent {
	return &Agent{reg, mgr, uGen, mach, ttl, map[string]*refCountedMux{}, new(sync.Mutex), &agentCache{}}
}

func (a *Agent) MarshalJSON() ([]byte, error) {
	data := struct {
		Cache *agentCache
	}{
		Cache: a.cache,
	}
	return json.Marshal(data)
}

// Heartbeat updates the Registry periodically with an acknowledgement of the
// Jobs this Agent is expected to be running.
func (a *Agent) Heartbeat(stop chan bool) {
	a.heartbeatJobs(a.ttl, stop)
}

func (a *Agent) heartbeatJobs(ttl time.Duration, stop chan bool) {
	heartbeat := func() {
		machID := a.Machine.State().ID
		launched := a.cache.launchedJobs()
		for _, j := range launched {
			go a.registry.UnitHeartbeat(j, machID, ttl)
		}
	}

	var interval time.Duration
	if ttl > 10*time.Second {
		interval = ttl * 4 / 5
	} else {
		interval = ttl / 2
	}

	ticker := time.Tick(interval)
	for {
		select {
		case <-stop:
			log.Debug("HeartbeatJobs exiting due to stop signal")
			return
		case <-ticker:
			log.Debug("HeartbeatJobs tick")
			heartbeat()
		}
	}
}

func (a *Agent) reloadUnitFiles() error {
	udplogger.Logln("systemd:ReloadUnitFiles")
	return a.um.ReloadUnitFiles()
}

func (a *Agent) loadUnit(u *job.Unit) error {
	a.cache.setTargetState(u.Name, job.JobStateLoaded)
	a.uGen.Subscribe(u.Name)
	udplogger.Logln("systemd:Load", u.Name)
	a.aquireUnitFileLock(u.Name)
	defer a.unlockUnitFile(u.Name)
	return a.um.Load(u.Name, u.Unit)
}

func (a *Agent) unloadUnit(unitName string) {
	a.registry.ClearUnitHeartbeat(unitName)
	a.cache.dropTargetState(unitName)

	udplogger.Logln("systemd:TriggerStop", unitName)
	a.um.TriggerStop(unitName)
	a.uGen.Unsubscribe(unitName)

	go func(unitName string) {
		a.aquireUnitFileLock(unitName)
		defer a.unlockUnitFile(unitName)

		period := 100 * time.Millisecond

		for retriesLeft := 50; retriesLeft > 0; retriesLeft-- {
			us, err := a.um.GetUnitState(unitName)
			if err == nil && (us.ActiveState == "inactive" || us.ActiveState == "failed") {
				break
			}
			time.Sleep(period)
		}

		udplogger.Logln("systemd:Unload", unitName)
		a.um.Unload(unitName)
	}(unitName)
}

func (a *Agent) startUnit(unitName string) {
	a.cache.setTargetState(unitName, job.JobStateLaunched)

	machID := a.Machine.State().ID
	a.registry.UnitHeartbeat(unitName, machID, a.ttl)

	udplogger.Logln("systemd:TriggerStart", unitName)
	a.um.TriggerStart(unitName)
}

func (a *Agent) stopUnit(unitName string) {
	a.cache.setTargetState(unitName, job.JobStateLoaded)
	a.registry.ClearUnitHeartbeat(unitName)

	udplogger.Logln("systemd:TriggerStop", unitName)
	a.um.TriggerStop(unitName)
}

type unitState struct {
	state job.JobState
	hash  string
}
type unitStates map[string]unitState

// units returns a map representing the current state of units known by the agent.
func (a *Agent) units() (unitStates, error) {
	launched := pkg.NewUnsafeSet()
	for _, jName := range a.cache.launchedJobs() {
		launched.Add(jName)
	}

	loaded := pkg.NewUnsafeSet()
	for _, jName := range a.cache.loadedJobs() {
		loaded.Add(jName)
	}

	units, err := a.um.Units()
	if err != nil {
		return nil, fmt.Errorf("failed fetching loaded units from UnitManager: %v", err)
	}

	filter := pkg.NewUnsafeSet()
	for _, u := range units {
		filter.Add(u)
	}

	uStates, err := a.um.GetUnitStates(filter)
	if err != nil {
		return nil, fmt.Errorf("failed fetching unit states from UnitManager: %v", err)
	}

	states := make(unitStates)
	for uName, uState := range uStates {
		js := job.JobStateInactive
		if loaded.Contains(uName) {
			js = job.JobStateLoaded
		} else if launched.Contains(uName) {
			js = job.JobStateLaunched
		}
		us := unitState{
			state: js,
			hash:  uState.UnitHash,
		}
		states[uName] = us
	}

	return states, nil
}

func (a *Agent) aquireUnitFileLock(unitName string) {
	a.unitFileMuxMux.Lock()
	l, exists := a.unitFileMux[unitName]
	if !exists {
		l = &refCountedMux{new(sync.Mutex), 0}
		a.unitFileMux[unitName] = l
	}
	l.refcount++
	a.unitFileMuxMux.Unlock()
	l.mu.Lock()
}

func (a *Agent) unlockUnitFile(unitName string) {
	a.unitFileMuxMux.Lock()
	defer a.unitFileMuxMux.Unlock()
	l, exists := a.unitFileMux[unitName]
	if !exists {
		return
	}
	l.refcount--
	if l.refcount == 0 {
		delete(a.unitFileMux, unitName)
	}
}
