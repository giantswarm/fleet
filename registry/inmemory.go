package registry

import (
	"sort"
	"sync"
	"time"

	pb "github.com/coreos/fleet/rpc"
)

type inmemoryRegistry struct {
	unitsCache     map[string]pb.Unit
	scheduledUnits map[string]pb.ScheduledUnit
	unitHeartbeats map[string]*unitHeartbeat
	mu             *sync.Mutex
}

func newInmemoryRegistry() *inmemoryRegistry {
	return &inmemoryRegistry{
		unitsCache:     map[string]pb.Unit{},
		scheduledUnits: map[string]pb.ScheduledUnit{},
		unitHeartbeats: map[string]*unitHeartbeat{},
		mu:             new(sync.Mutex),
	}

}

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
	units = make([]pb.ScheduledUnit, 0, len(r.scheduledUnits))
	for _, schedUnit := range r.scheduledUnits {
		su := schedUnit
		su.CurrentState = r.getScheduledUnitState(su.Name)
		units = append(units, su)
	}
	return units, nil
}

func (r *inmemoryRegistry) ScheduledUnit(name string) (unit *pb.ScheduledUnit, exists bool) {
	if schedUnit, exists := r.scheduledUnits[name]; exists {
		su := &schedUnit
		su.CurrentState = r.getScheduledUnitState(name)
		return su, true
	}
	return nil, false
}

func (r *inmemoryRegistry) Unit(name string) (pb.Unit, bool) {
	u, exists := r.unitsCache[name]
	return u, exists
}

func (r *inmemoryRegistry) Units() []pb.Unit {
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
	if _, exists := r.unitHeartbeats[name]; exists {
		r.unitHeartbeats[name].launchedDeadline = time.Now()
	}
}

func (r *inmemoryRegistry) DestroyUnit(name string) bool {
	if _, exists := r.unitsCache[name]; exists {
		delete(r.unitsCache, name)
		return true
	}
	return false
}

func (r *inmemoryRegistry) RemoveUnitState(unitname string) {
	if _, exists := r.unitHeartbeats[unitname]; exists {
		delete(r.unitHeartbeats, unitname)
	}
}

func (r *inmemoryRegistry) SaveUnitState(unitname string, state *pb.UnitState, ttl time.Duration) {
	r.unitHeartbeats[unitname] = &unitHeartbeat{
		state:    state,
		machine:  state.Machine,
		deadline: time.Now().Add(ttl),
	}
}

func (r *inmemoryRegistry) UnitHeartbeat(unitname, machineid string, ttl time.Duration) {
	if _, exists := r.unitHeartbeats[unitname]; exists {
		r.unitHeartbeats[unitname].beatLaunched(machineid, ttl)
	}
}

func (r *inmemoryRegistry) ScheduleUnit(unitname, machineid string) {
	r.scheduledUnits[unitname] = pb.ScheduledUnit{
		Name:         unitname,
		CurrentState: pb.TargetState_INACTIVE,
		Machine:      machineid,
	}
}

func (r *inmemoryRegistry) UnscheduleUnit(unitname, machineid string) {
	delete(r.scheduledUnits, unitname)
}

func (r *inmemoryRegistry) SetUnitTargetState(unitname string, targetState pb.TargetState) bool {
	if u, exists := r.unitsCache[unitname]; exists {
		u.DesiredState = targetState
		r.unitsCache[unitname] = u
		return true
	}
	return false
}

func (r *inmemoryRegistry) CreateUnit(u *pb.Unit) {
	r.unitsCache[u.Name] = *u
}

func (r *inmemoryRegistry) statesByMUSKey() map[MUSKey]*pb.UnitState {
	states := map[MUSKey]*pb.UnitState{}
	for name, heartbeat := range r.unitHeartbeats {
		if !heartbeat.isValid() {
			continue
		}
		k := MUSKey{
			name:   name,
			machID: heartbeat.machine,
		}

		state := *heartbeat.state

		states[k] = &state
	}
	return states
}

func (r *inmemoryRegistry) getScheduledUnitState(unitName string) pb.TargetState {
	if heartbeat, hasHeartbeat := r.unitHeartbeats[unitName]; hasHeartbeat {
		if r.isScheduled(unitName, heartbeat.machine) {
			if heartbeat.isLaunchedValid() {
				return pb.TargetState_LAUNCHED
			}
			if heartbeat.isValid() {
				return pb.TargetState_LOADED
			}
		}
	}
	return pb.TargetState_INACTIVE
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
