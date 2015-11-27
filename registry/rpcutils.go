package registry

import (
	"fmt"
	"time"

	sdunit "github.com/coreos/go-systemd/unit"

	"github.com/coreos/fleet/job"
	pb "github.com/coreos/fleet/rpc"
	"github.com/coreos/fleet/unit"
)

type unitHeartbeat struct {
	deadline         time.Time
	launchedDeadline time.Time
	state            *pb.UnitState
	machine          string
}

func (u unitHeartbeat) isValid() bool {
	return u.deadline.After(time.Now())
}

func (u unitHeartbeat) isLaunchedValid() bool {
	return u.deadline.After(time.Now()) && u.launchedDeadline.After(time.Now())
}

func (u *unitHeartbeat) beat(machine string, ttl time.Duration) {
	fmt.Println("XDATA beat", u.state.Name, machine)
	u.deadline = time.Now().Add(ttl)
	u.machine = machine
}

func (u *unitHeartbeat) beatLaunched(machine string, ttl time.Duration) {
	fmt.Println("XDATA beatLaunched", u.state.Name, machine)
	u.deadline = time.Now().Add(ttl)
	u.launchedDeadline = time.Now().Add(ttl)
	u.machine = machine
}

func (s *rpcserver) notifyMachine(machine string, units []string) {
	if ch, exists := s.machinesDirectory[machine]; exists {
		ch <- units
	}
}

func (s *rpcserver) notifyAllMachines(units []string) {
	for _, ch := range s.machinesDirectory {
		ch <- units
	}
}

func rpcUnitStateToJobState(state pb.TargetState) job.JobState {
	switch state {
	case pb.TargetState_INACTIVE:
		return job.JobStateInactive
	case pb.TargetState_LOADED:
		return job.JobStateLoaded
	case pb.TargetState_LAUNCHED:
		return job.JobStateLaunched
	}
	return job.JobStateInactive
}

func rpcUnitStateToExtUnitState(state *pb.UnitState) *unit.UnitState {
	return &unit.UnitState{
		UnitName:    state.Name,
		UnitHash:    state.Hash,
		LoadState:   state.LoadState,
		ActiveState: state.ActiveState,
		SubState:    state.SubState,
		MachineID:   state.Machine,
	}
}

func rpcUnitToJobUnit(u *pb.Unit) *job.Unit {
	unitOptions := make([]*sdunit.UnitOption, len(u.Unit.UnitOptions))

	for i, option := range u.Unit.UnitOptions {
		unitOptions[i] = &sdunit.UnitOption{
			Section: option.Section,
			Name:    option.Name,
			Value:   option.Value,
		}
	}

	return &job.Unit{
		Name:        u.Name,
		Unit:        *unit.NewUnitFromOptions(unitOptions),
		TargetState: rpcUnitStateToJobState(u.DesiredState),
	}
}
