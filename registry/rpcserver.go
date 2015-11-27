package registry

import (
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	pb "github.com/coreos/fleet/rpc"
	"google.golang.org/grpc"

	"strings"

	"sort"
)

type machineChan chan []string

type rpcserver struct {
	etcdRegistry      *EtcdRegistry
	machinesDirectory map[string]machineChan
	mu                *sync.Mutex

	unitsCache     map[string]pb.Unit
	scheduledUnits map[string]pb.ScheduledUnit
	unitHeartbeats map[string]*unitHeartbeat
	cacheMu        *sync.Mutex
}

func (r *RPCRegistry) startServer() {
	if r.listener != nil {
		r.listener.Close()
	}
	var err error
	r.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()

	rpcS := &rpcserver{
		etcdRegistry:      r.etcdRegistry,
		machinesDirectory: map[string]machineChan{},
		mu:                new(sync.Mutex),

		unitsCache:     map[string]pb.Unit{},
		scheduledUnits: map[string]pb.ScheduledUnit{},
		unitHeartbeats: map[string]*unitHeartbeat{},
		cacheMu:        new(sync.Mutex),
	}

	r.done = make(chan struct{})

	rpcS.cacheUnits()
	rpcS.cacheSchedule()

	pb.RegisterRegistryServer(s, rpcS)
	go s.Serve(r.listener)
	go func() {
		<-r.done
		s.Stop()
	}()
}

func (s *rpcserver) GetScheduledUnits(ctx context.Context, unitFilter *pb.UnitFilter) (*pb.ScheduledUnits, error) {
	fmt.Println("XENGINE", "GetScheduledUnits", unitFilter)
	if 1 == 2 {
		schedule, err := s.etcdRegistry.Schedule()
		if err != nil {
			return nil, err
		}

		units := make([]pb.ScheduledUnit, len(schedule))
		for i, u := range schedule {
			units[i] = u.ToPB()
		}

		return &pb.ScheduledUnits{
			Units: units,
		}, nil
	}

	units := make([]pb.ScheduledUnit, 0, len(s.scheduledUnits))
	for _, schedUnit := range s.scheduledUnits {
		su := schedUnit
		su.CurrentState = s.getScheduledUnitState(su.Name)
		units = append(units, su)
	}

	return &pb.ScheduledUnits{
		Units: units,
	}, nil

}

func (s *rpcserver) GetScheduledUnit(ctx context.Context, name *pb.UnitName) (*pb.MaybeScheduledUnit, error) {
	fmt.Println("XENGINE", "GetScheduledUnit", name)
	if 1 == 2 {
		scheduledUnit, err := s.etcdRegistry.ScheduledUnit(name.Name)
		if err != nil {
			return nil, err
		}
		scheduled := scheduledUnit.ToPB()
		return &pb.MaybeScheduledUnit{IsScheduled: &pb.MaybeScheduledUnit_Unit{&scheduled}}, nil
	}

	if schedUnit, exists := s.scheduledUnits[name.Name]; exists {
		su := &schedUnit
		su.CurrentState = s.getScheduledUnitState(name.Name)
		return &pb.MaybeScheduledUnit{IsScheduled: &pb.MaybeScheduledUnit_Unit{su}}, nil
	}
	return &pb.MaybeScheduledUnit{IsScheduled: &pb.MaybeScheduledUnit_Notfound{Notfound: &pb.NotFound{}}}, nil
}

func (s *rpcserver) GetUnit(ctx context.Context, name *pb.UnitName) (*pb.MaybeUnit, error) {
	fmt.Println("XENGINE", "GetUnit", name)
	if 1 == 2 {

		u, err := s.etcdRegistry.Unit(name.Name)
		if err != nil {
			return nil, err
		}

		if u == nil {
			return &pb.MaybeUnit{HasUnit: &pb.MaybeUnit_Notfound{Notfound: &pb.NotFound{}}}, nil
		}
		un := u.ToPB()
		return &pb.MaybeUnit{HasUnit: &pb.MaybeUnit_Unit{&un}}, nil
	}

	xu, _ := s.etcdRegistry.Unit(name.Name)

	if unit, exists := s.unitsCache[name.Name]; exists {
		fmt.Println("XDATA", "cache GET ", name.Name, unit, xu)
		return &pb.MaybeUnit{HasUnit: &pb.MaybeUnit_Unit{&unit}}, nil
	}
	return &pb.MaybeUnit{HasUnit: &pb.MaybeUnit_Notfound{Notfound: &pb.NotFound{}}}, nil

}

func (s *rpcserver) GetUnits(ctx context.Context, filter *pb.UnitFilter) (*pb.Units, error) {
	fmt.Println("XENGINE", "GetUnits", filter)
	if 1 == 2 {
		units, err := s.etcdRegistry.Units()
		if err != nil {
			return nil, err
		}

		rpcUnits := make([]pb.Unit, len(units))
		for idx, unit := range units {
			un := unit.ToPB()
			rpcUnits[idx] = un
		}

		fmt.Println("XENGINE", rpcUnits)

		return &pb.Units{Units: rpcUnits}, nil
	}

	units := make([]pb.Unit, len(s.unitsCache))
	unitNames := make([]string, 0, len(s.unitsCache))
	for k, _ := range s.unitsCache {
		unitNames = append(unitNames, k)
	}
	fmt.Println("XDATA", "cache GETALL ", len(unitNames))
	sort.Strings(unitNames)
	for _, unitName := range unitNames {
		u := s.unitsCache[unitName]
		units = append(units, u)
	}
	return &pb.Units{Units: units}, nil
}

func (s *rpcserver) GetUnitStates(ctx context.Context, filter *pb.UnitStateFilter) (*pb.UnitStates, error) {
	fmt.Println("XENGINE", "GetUnitStates", filter)
	if 1 == 2 {
		unitStates, err := s.etcdRegistry.UnitStates()
		if err != nil {
			return nil, err
		}

		rpcUnitStates := make([]*pb.UnitState, len(unitStates))
		for idx, unitState := range unitStates {
			rpcUnitStates[idx] = unitState.ToPB()
		}

		return &pb.UnitStates{rpcUnitStates}, nil
	}

	states := []*pb.UnitState{}
	var mus map[MUSKey]*pb.UnitState
	mus, err := s.statesByMUSKey()
	if err != nil {
		return &pb.UnitStates{states}, err
	}

	var sorted MUSKeys
	for key, _ := range mus {
		sorted = append(sorted, key)
	}
	sort.Sort(sorted)

	for _, key := range sorted {
		states = append(states, mus[key])
	}

	return &pb.UnitStates{states}, nil
}

func (s *rpcserver) statesByMUSKey() (map[MUSKey]*pb.UnitState, error) {
	states := map[MUSKey]*pb.UnitState{}
	for name, heartbeat := range s.unitHeartbeats {
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
	return states, nil
}

func (s *rpcserver) ClearUnitHeartbeat(ctx context.Context, name *pb.UnitName) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "ClearUnitHeartbeat", name)
	//s.etcdRegistry.ClearUnitHeartbeat(name.Name)
	if _, exists := s.unitHeartbeats[name.Name]; exists {
		s.unitHeartbeats[name.Name].launchedDeadline = time.Now()
	}
	return &pb.GenericReply{}, nil
}

func (s *rpcserver) CreateUnit(ctx context.Context, u *pb.Unit) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "CreateUnit", u)
	err := s.etcdRegistry.CreateUnit(rpcUnitToJobUnit(u))
	if err == nil {
		unit := *u
		s.unitsCache[u.Name] = unit
		fmt.Println("XDATA", "cache SET ", u.Name)
	}
	return &pb.GenericReply{}, err
}

func (s *rpcserver) DestroyUnit(ctx context.Context, name *pb.UnitName) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "DestroyUnit", name)
	err := s.etcdRegistry.DestroyUnit(name.Name)
	if err == nil {
		delete(s.unitsCache, name.Name)
		fmt.Println("XDATA", "cache DELETE ", name.Name)
	}
	return &pb.GenericReply{}, err
}

func (s *rpcserver) UnitHeartbeat(ctx context.Context, heartbeat *pb.Heartbeat) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "Heartbeat", heartbeat)
	//err := s.etcdRegistry.UnitHeartbeat(heartbeat.Name, heartbeat.Machine, time.Duration(heartbeat.Ttl)*time.Second)
	if _, exists := s.unitHeartbeats[heartbeat.Name]; exists {
		s.unitHeartbeats[heartbeat.Name].beatLaunched(heartbeat.Machine, time.Duration(heartbeat.TTL)*time.Second)
	}

	return &pb.GenericReply{}, nil
}

func (s *rpcserver) RemoveUnitState(ctx context.Context, name *pb.UnitName) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "RemoveUnitState", name)
	//err := s.etcdRegistry.RemoveUnitState(name.Name)
	if _, exists := s.unitHeartbeats[name.Name]; exists {
		delete(s.unitHeartbeats, name.Name)
	}
	return &pb.GenericReply{}, nil
}

func (s *rpcserver) SaveUnitState(ctx context.Context, req *pb.SaveUnitStateRequest) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "SaveUnitState", req)
	//s.etcdRegistry.SaveUnitState(req.Name, rpcUnitStateToExtUnitState(req.State), time.Duration(req.TTL)*time.Second)
	s.unitHeartbeats[req.Name] = &unitHeartbeat{
		state:    req.State,
		machine:  req.State.Machine,
		deadline: time.Now().Add(time.Second * time.Duration(req.TTL)),
	}
	return &pb.GenericReply{}, nil
}

func (s *rpcserver) ScheduleUnit(ctx context.Context, unit *pb.ScheduleUnitRequest) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "ScheduleUnit", unit)
	err := s.etcdRegistry.ScheduleUnit(unit.Name, unit.Machine)
	if err == nil {
		s.scheduledUnits[unit.Name] = pb.ScheduledUnit{
			Name:         unit.Name,
			CurrentState: pb.TargetState_INACTIVE,
			Machine:      unit.Machine,
		}

		s.notifyMachine(unit.Machine, []string{unit.Name})
	}
	return &pb.GenericReply{}, err
}

func (s *rpcserver) SetUnitTargetState(ctx context.Context, unit *pb.ScheduledUnit) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "SetUnitTargetState", unit)
	err := s.etcdRegistry.SetUnitTargetState(unit.Name, rpcUnitStateToJobState(unit.CurrentState))
	if err == nil {
		if u, exists := s.unitsCache[unit.Name]; exists {
			u.DesiredState = unit.CurrentState
			s.unitsCache[unit.Name] = u
			s.notifyAllMachines([]string{unit.Name})
		}

	}
	return &pb.GenericReply{}, err
}

func (s *rpcserver) UnscheduleUnit(ctx context.Context, unit *pb.UnscheduleUnitRequest) (*pb.GenericReply, error) {
	fmt.Println("XENGINE", "UnscheduleUnit", unit)
	err := s.etcdRegistry.UnscheduleUnit(unit.Name, unit.Machine)
	if err == nil {
		delete(s.scheduledUnits, unit.Name)
		s.notifyMachine(unit.Machine, []string{unit.Name})
	}
	return &pb.GenericReply{}, err
}
func (s *rpcserver) Identify(ctx context.Context, props *pb.MachineProperties) (*pb.GenericReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//	s.machinesDirectory[props.Id] =
	return nil, nil
}

func (s *rpcserver) AgentEvents(props *pb.MachineProperties, stream pb.Registry_AgentEventsServer) error {
	s.mu.Lock()
	ch := make(machineChan)
	s.machinesDirectory[strings.ToLower(props.Id)] = ch
	s.mu.Unlock()
	for updatedUnits := range ch {
		err := stream.Send(&pb.UpdatedState{updatedUnits})
		if err != nil {
			return err
		}
	}
	return nil
}
