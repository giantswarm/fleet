package registry

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/coreos/fleet/debug"
	pb "github.com/coreos/fleet/rpc"
)

type inmemoryRegistry struct {
	unitsCache     map[string]pb.Unit
	scheduledUnits map[string]pb.ScheduledUnit
	unitHeartbeats map[string]map[string]time.Time
	unitStates     map[string]map[string]*unitStateHeartbeat
	mu             *sync.RWMutex
	heartbeatsMu   *sync.RWMutex
	unitStatesMu   *sync.RWMutex
}

func newInmemoryRegistry() *inmemoryRegistry {
	r := &inmemoryRegistry{
		unitsCache:     map[string]pb.Unit{},
		scheduledUnits: map[string]pb.ScheduledUnit{},
		unitHeartbeats: map[string]map[string]time.Time{},
		unitStates:     map[string]map[string]*unitStateHeartbeat{},
		mu:             new(sync.RWMutex),
		heartbeatsMu:   new(sync.RWMutex),
		unitStatesMu:   new(sync.RWMutex),
	}

	if debug.Enabled {
		latestReg = r
	}

	return r
}

func init() {
	debug.Enabled = true
	if debug.Enabled {
		http.Handle("/fleet/inmemory", http.HandlerFunc(dbgHandler))
	}
}

func dbgHandler(w http.ResponseWriter, r *http.Request) {
	if latestReg == nil {
		return
	}
	e := json.NewEncoder(w)

	data := map[string]interface{}{
		"Units":          latestReg.unitsCache,
		"ScheduledUnits": latestReg.scheduledUnits,
		"Heartbeats":     latestReg.unitHeartbeats,
		"States":         latestReg.unitStates,
	}

	e.Encode(data)
}

var latestReg *inmemoryRegistry

func (r *inmemoryRegistry) LoadFrom(reg UnitRegistry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	units, err := reg.Units()
	if err != nil {
		return err
	}
	for _, u := range units {
		r.unitsCache[u.Name] = u.ToPB()
	}

	schedule, err := reg.Schedule()
	if err != nil {
		return err
	}

	for _, scheduledUnit := range schedule {
		r.scheduledUnits[scheduledUnit.Name] = scheduledUnit.ToPB()
	}

	return nil
}

func (r *inmemoryRegistry) Schedule() (units []pb.ScheduledUnit, err error) {
	defer debug.Exit_(debug.Enter_())
	r.mu.RLock()
	defer r.mu.RUnlock()

	units = make([]pb.ScheduledUnit, 0, len(r.scheduledUnits))
	for _, schedUnit := range r.scheduledUnits {
		su := schedUnit
		su.CurrentState = r.getScheduledUnitState(su.Name, su.Machine)
		units = append(units, su)
	}
	return units, nil
}

func (r *inmemoryRegistry) ScheduledUnit(name string) (unit *pb.ScheduledUnit, exists bool) {
	defer debug.Exit_(debug.Enter_(name))
	r.mu.RLock()
	defer r.mu.RUnlock()

	if schedUnit, exists := r.scheduledUnits[name]; exists {
		su := &schedUnit
		su.CurrentState = r.getScheduledUnitState(name, schedUnit.Machine)
		return su, true
	}
	return nil, false
}

func (r *inmemoryRegistry) Unit(name string) (pb.Unit, bool) {
	defer debug.Exit_(debug.Enter_(name))
	r.mu.RLock()
	defer r.mu.RUnlock()

	u, exists := r.unitsCache[name]
	return u, exists
}

func (r *inmemoryRegistry) Units() []pb.Unit {
	defer debug.Exit_(debug.Enter_())
	r.mu.RLock()
	defer r.mu.RUnlock()

	units := make([]pb.Unit, len(r.unitsCache))
	unitNames := make([]string, 0, len(r.unitsCache))
	for k, _ := range r.unitsCache {
		unitNames = append(unitNames, k)
	}
	sort.Strings(unitNames)
	for _, unitName := range unitNames {
		u := r.unitsCache[unitName]
		units = append(units, u)
	}
	return units
}

func (r *inmemoryRegistry) UnitStates() []*pb.UnitState {
	defer debug.Exit_(debug.Enter_())
	r.unitStatesMu.Lock()
	defer r.unitStatesMu.Unlock()

	states := []*pb.UnitState{}
	mus := r.statesByMUSKey()

	var sorted MUSKeys
	for key, _ := range mus {
		sorted = append(sorted, key)
	}
	sort.Sort(sorted)

	for _, key := range sorted {
		states = append(states, mus[key])
	}

	return states
}

func (r *inmemoryRegistry) ClearUnitHeartbeat(name string) {
	defer debug.Exit_(debug.Enter_(name))
	r.heartbeatsMu.Lock()
	defer r.heartbeatsMu.Unlock()

	if _, exists := r.unitHeartbeats[name]; exists {
		delete(r.unitHeartbeats, name)
	}
}

func (r *inmemoryRegistry) DestroyUnit(name string) bool {
	defer debug.Exit_(debug.Enter_(name))
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unitStatesMu.Lock()
	defer r.unitStatesMu.Unlock()
	r.heartbeatsMu.Lock()
	defer r.heartbeatsMu.Unlock()

	deleted := false

	if _, exists := r.unitsCache[name]; exists {
		delete(r.unitsCache, name)
		deleted = true
	}

	if _, exists := r.scheduledUnits[name]; exists {
		delete(r.scheduledUnits, name)
	}

	if _, exists := r.unitHeartbeats[name]; exists {
		delete(r.unitHeartbeats, name)
	}

	if _, exists := r.unitStates[name]; exists {
		delete(r.unitStates, name)
	}

	return deleted
}

func (r *inmemoryRegistry) RemoveUnitState(unitname string) {
	defer debug.Exit_(debug.Enter_(unitname))
	r.unitStatesMu.Lock()
	defer r.unitStatesMu.Unlock()

	if _, exists := r.unitStates[unitname]; exists {
		delete(r.unitStates, unitname)
	}
}

func (r *inmemoryRegistry) SaveUnitState(unitname string, state *pb.UnitState, ttl time.Duration) {
	defer debug.Exit_(debug.Enter_(unitname, state))
	r.unitStatesMu.Lock()
	defer r.unitStatesMu.Unlock()

	statebeat := &unitStateHeartbeat{
		state:    state,
		deadline: time.Now().Add(ttl),
	}

	if _, exists := r.unitStates[unitname]; exists {
		r.unitStates[unitname][state.Machine] = statebeat
	} else {
		r.unitStates[unitname] = map[string]*unitStateHeartbeat{state.Machine: statebeat}
	}

}

func (r *inmemoryRegistry) UnitHeartbeat(unitname, machineid string, ttl time.Duration) {
	defer debug.Exit_(debug.Enter_(unitname, machineid, ttl))
	r.heartbeatsMu.Lock()
	defer r.heartbeatsMu.Unlock()

	if _, exists := r.unitHeartbeats[unitname]; exists {
		r.unitHeartbeats[unitname][machineid] = time.Now().Add(ttl)
	} else {
		r.unitHeartbeats[unitname] = map[string]time.Time{machineid: time.Now().Add(ttl)}
	}
}

func (r *inmemoryRegistry) ScheduleUnit(unitname, machineid string) {
	defer debug.Exit_(debug.Enter_(unitname, machineid))
	r.mu.Lock()
	defer r.mu.Unlock()

	r.scheduledUnits[unitname] = pb.ScheduledUnit{
		Name:         unitname,
		CurrentState: pb.TargetState_INACTIVE,
		Machine:      machineid,
	}
}

func (r *inmemoryRegistry) UnscheduleUnit(unitname, machineid string) {
	defer debug.Exit_(debug.Enter_(unitname, machineid))
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.scheduledUnits, unitname)
}

func (r *inmemoryRegistry) SetUnitTargetState(unitname string, targetState pb.TargetState) bool {
	defer debug.Exit_(debug.Enter_(unitname, targetState))
	r.mu.Lock()
	defer r.mu.Unlock()

	if u, exists := r.unitsCache[unitname]; exists {
		u.DesiredState = targetState
		r.unitsCache[unitname] = u
		return true
	}
	return false
}

func (r *inmemoryRegistry) CreateUnit(u *pb.Unit) {
	defer debug.Exit_(debug.Enter_(u))
	r.mu.Lock()
	defer r.mu.Unlock()

	r.unitsCache[u.Name] = *u
}

func (r *inmemoryRegistry) statesByMUSKey() map[MUSKey]*pb.UnitState {
	states := map[MUSKey]*pb.UnitState{}

	for unitname, unitStates := range r.unitStates {
		for machineID, heartbeat := range unitStates {
			if heartbeat.isValid() {
				k := MUSKey{
					name:   unitname,
					machID: machineID,
				}
				s := *heartbeat.state
				states[k] = &s

			}
		}
	}

	return states
}

func (r *inmemoryRegistry) isUnitLoaded(unitName, machineID string) bool {
	if _, exists := r.unitStates[unitName]; exists {
		if _, exists := r.unitStates[unitName][machineID]; exists {
			return true
		}
	}
	return false
}

func (r *inmemoryRegistry) isUnitLaunched(unitName, machineID string) bool {
	if _, exists := r.unitHeartbeats[unitName]; exists {
		if _, exists := r.unitHeartbeats[unitName][machineID]; exists {
			return true
		}
	}
	return false
}

func (r *inmemoryRegistry) getScheduledUnitState(unitName, machineID string) pb.TargetState {
	if r.isUnitLoaded(unitName, machineID) {
		if r.isUnitLaunched(unitName, machineID) {
			return pb.TargetState_LAUNCHED
		} else {
			return pb.TargetState_LOADED
		}
	} else {
		return pb.TargetState_INACTIVE
	}
}

func (r *inmemoryRegistry) isScheduled(unitName, machine string) bool {
	if machine == "" || unitName == "" {
		return false
	}
	if s, exists := r.scheduledUnits[unitName]; exists {
		return s.Machine == machine
	}
	return false
}

type unitStateHeartbeat struct {
	deadline time.Time
	state    *pb.UnitState
}

type unitHeartbeat struct {
	deadline         time.Time
	launchedDeadline time.Time
	state            *pb.UnitState
	machine          string
}

func (u *unitStateHeartbeat) isValid() bool {
	return u.deadline.After(time.Now())
}

func (u *unitStateHeartbeat) beat(machine string, ttl time.Duration) {
	u.deadline = time.Now().Add(ttl)
}
