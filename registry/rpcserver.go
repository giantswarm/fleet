package registry

import (
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/fleet/debug"
	pb "github.com/coreos/fleet/rpc"
	"google.golang.org/grpc"

	"strings"
)

type machineChan chan []string

type rpcserver struct {
	etcdRegistry      *EtcdRegistry
	machinesDirectory map[string]machineChan
	mu                *sync.Mutex

	localRegistry *inmemoryRegistry
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

		localRegistry: newInmemoryRegistry(),
	}

	r.done = make(chan struct{})

	rpcS.localRegistry.LoadFrom(r.etcdRegistry)

	pb.RegisterRegistryServer(s, rpcS)
	go s.Serve(r.listener)
	go func() {
		<-r.done
		s.Stop()
	}()
}

func (s *rpcserver) GetScheduledUnits(ctx context.Context, unitFilter *pb.UnitFilter) (*pb.ScheduledUnits, error) {
	defer debug.Exit_(debug.Enter_())
	units, err := s.localRegistry.Schedule()

	return &pb.ScheduledUnits{Units: units}, err
}

func (s *rpcserver) GetScheduledUnit(ctx context.Context, name *pb.UnitName) (*pb.MaybeScheduledUnit, error) {
	defer debug.Exit_(debug.Enter_(name.Name))

	su, exists := s.localRegistry.ScheduledUnit(name.Name)
	if exists {
		return &pb.MaybeScheduledUnit{IsScheduled: &pb.MaybeScheduledUnit_Unit{su}}, nil
	}
	return &pb.MaybeScheduledUnit{IsScheduled: &pb.MaybeScheduledUnit_Notfound{Notfound: &pb.NotFound{}}}, nil
}

func (s *rpcserver) GetUnit(ctx context.Context, name *pb.UnitName) (*pb.MaybeUnit, error) {
	defer debug.Exit_(debug.Enter_(name.Name))

	unit, exists := s.localRegistry.Unit(name.Name)
	if exists {
		return &pb.MaybeUnit{HasUnit: &pb.MaybeUnit_Unit{&unit}}, nil
	}
	return &pb.MaybeUnit{HasUnit: &pb.MaybeUnit_Notfound{Notfound: &pb.NotFound{}}}, nil

}

func (s *rpcserver) GetUnits(ctx context.Context, filter *pb.UnitFilter) (*pb.Units, error) {
	defer debug.Exit_(debug.Enter_())

	units := s.localRegistry.Units()
	return &pb.Units{Units: units}, nil
}

func (s *rpcserver) GetUnitStates(ctx context.Context, filter *pb.UnitStateFilter) (*pb.UnitStates, error) {
	defer debug.Exit_(debug.Enter_())

	states := s.localRegistry.UnitStates()

	return &pb.UnitStates{states}, nil
}

func (s *rpcserver) ClearUnitHeartbeat(ctx context.Context, name *pb.UnitName) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(name.Name))

	s.localRegistry.ClearUnitHeartbeat(name.Name)
	return &pb.GenericReply{}, nil
}

func (s *rpcserver) CreateUnit(ctx context.Context, u *pb.Unit) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(u.Name))

	err := s.etcdRegistry.CreateUnit(rpcUnitToJobUnit(u))
	if err == nil {
		s.localRegistry.CreateUnit(u)
	}
	return &pb.GenericReply{}, err
}

func (s *rpcserver) DestroyUnit(ctx context.Context, name *pb.UnitName) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(name.Name))

	err := s.etcdRegistry.DestroyUnit(name.Name)
	if err == nil {
		s.localRegistry.DestroyUnit(name.Name)
	}
	return &pb.GenericReply{}, err
}

func (s *rpcserver) UnitHeartbeat(ctx context.Context, heartbeat *pb.Heartbeat) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(heartbeat))

	s.localRegistry.UnitHeartbeat(heartbeat.Name, heartbeat.Machine, time.Duration(heartbeat.TTL)*time.Second)
	return &pb.GenericReply{}, nil
}

func (s *rpcserver) RemoveUnitState(ctx context.Context, name *pb.UnitName) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(name.Name))

	s.localRegistry.RemoveUnitState(name.Name)
	return &pb.GenericReply{}, nil
}

func (s *rpcserver) SaveUnitState(ctx context.Context, req *pb.SaveUnitStateRequest) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(req))

	s.localRegistry.SaveUnitState(req.Name, req.State, time.Duration(req.TTL)*time.Second)
	return &pb.GenericReply{}, nil
}

func (s *rpcserver) ScheduleUnit(ctx context.Context, unit *pb.ScheduleUnitRequest) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(unit.Name, unit.Machine))

	err := s.etcdRegistry.ScheduleUnit(unit.Name, unit.Machine)
	if err == nil {
		s.localRegistry.ScheduleUnit(unit.Name, unit.Machine)
		s.notifyMachine(unit.Machine, []string{unit.Name})
	}
	return &pb.GenericReply{}, err
}

func (s *rpcserver) SetUnitTargetState(ctx context.Context, unit *pb.ScheduledUnit) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(unit.Name, unit.CurrentState))

	err := s.etcdRegistry.SetUnitTargetState(unit.Name, rpcUnitStateToJobState(unit.CurrentState))
	if err == nil {
		if s.localRegistry.SetUnitTargetState(unit.Name, unit.CurrentState) {
			s.notifyAllMachines([]string{unit.Name})
		}
	}
	return &pb.GenericReply{}, err
}

func (s *rpcserver) UnscheduleUnit(ctx context.Context, unit *pb.UnscheduleUnitRequest) (*pb.GenericReply, error) {
	defer debug.Exit_(debug.Enter_(unit.Name, unit.Machine))

	err := s.etcdRegistry.UnscheduleUnit(unit.Name, unit.Machine)
	if err == nil {
		s.localRegistry.UnscheduleUnit(unit.Name, unit.Machine)
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
	defer debug.Exit_(debug.Enter_(props.Id))

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
