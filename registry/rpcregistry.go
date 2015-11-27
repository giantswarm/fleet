package registry

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/coreos/fleet/job"
	"github.com/coreos/fleet/machine"
	"github.com/coreos/fleet/pkg"
	"github.com/coreos/fleet/rpc"
	"github.com/coreos/fleet/unit"
	"github.com/coreos/go-semver/semver"
)

const (
	port = 50059
)

type RPCRegistry struct {
	etcdRegistry         *EtcdRegistry
	leaderUpdateNotifier chan string
	localMachine         *machine.CoreOSMachine
	currentLeader        string
	listener             net.Listener
	registryClient       rpc.RegistryClient
	registryConn         *grpc.ClientConn
	eventListeners       map[chan pkg.Event]struct{}
	mu                   *sync.Mutex
	server               *grpc.Server

	done chan struct{}
}

type agentEvent struct {
	units []string
}

func NewRPCRegistry(etcdRegistry *EtcdRegistry, leaderUpdateNotifier chan string, mach *machine.CoreOSMachine) *RPCRegistry {
	return &RPCRegistry{
		etcdRegistry:         etcdRegistry,
		leaderUpdateNotifier: leaderUpdateNotifier,
		localMachine:         mach,
		eventListeners:       map[chan pkg.Event]struct{}{},
		mu:                   new(sync.Mutex),
		currentLeader:        "",
	}
}

func (r *RPCRegistry) NewEventStream() pkg.EventStream {
	return r
}

func (r *RPCRegistry) Next(stop chan struct{}) chan pkg.Event {
	ch := make(chan pkg.Event)
	r.mu.Lock()
	r.eventListeners[ch] = struct{}{}
	r.mu.Unlock()
	go func() {
		<-stop
		r.mu.Lock()
		delete(r.eventListeners, ch)
		r.mu.Unlock()
	}()
	return ch
}

func (r *RPCRegistry) broadcastEvent(ev agentEvent) {
	for ch, _ := range r.eventListeners {
		ch <- "newEvent"
	}
}

func (r *RPCRegistry) Start() {
	for leaderUpdate := range r.leaderUpdateNotifier {
		if r.currentLeader != leaderUpdate {
			r.currentLeader = leaderUpdate
			fmt.Println("XXX got new leader", leaderUpdate)
			if r.currentLeader == r.localMachine.String() {
				fmt.Println("XXX local machine is leader, doing things ")
				r.startServer()
			} else {
				if r.listener != nil {
					r.listener.Close()
				}
			}
			if r.registryConn != nil {
				r.registryConn.Close()
			}
			addr := fmt.Sprintf("%s:%d", r.findMachineAddr(r.currentLeader), port)
			var err error
			r.registryConn, err = grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("nope: %s", err)
			}
			r.registryClient = rpc.NewRegistryClient(r.registryConn)
			go func() {
				for {
					eventsStream, err := r.getClient().AgentEvents(context.Background(), &rpc.MachineProperties{r.localMachine.String()})
					if err != nil {
						continue
					}
					for {
						events, err := eventsStream.Recv()
						if err == io.EOF {
							break
						}
						if err != nil {
							break
						}
						r.broadcastEvent(agentEvent{events.UnitIds})
					}
				}
			}()
		}
	}
}

func (r *RPCRegistry) findMachineAddr(machineID string) string {
	machines, err := r.etcdRegistry.Machines()
	if err != nil {
		log.Println("err: unable to get machines:", err)
		return ""
	}
	for _, machine := range machines {
		if machine.ID == machineID {
			return machine.PublicIP
		}
	}
	log.Println("err: unable to find the right machine ")
	return ""
}

func (r *RPCRegistry) getClient() rpc.RegistryClient {
	for ; ; time.Sleep(100 * time.Millisecond) {
		if r.registryClient != nil {
			break
		}
	}
	return r.registryClient
}

func (r *RPCRegistry) ClearUnitHeartbeat(name string) {
	fmt.Println("XRPCC", "ClearUnitHeartbeat()", name)
	//r.etcdRegistry.ClearUnitHeartbeat(name)
	//return
	r.getClient().ClearUnitHeartbeat(context.Background(), &rpc.UnitName{name})
}

func (r *RPCRegistry) CreateUnit(j *job.Unit) error {
	fmt.Println("XRPCC", "CreateUnit()", j)
	//return r.etcdRegistry.CreateUnit(j)
	un := j.ToPB()
	_, err := r.getClient().CreateUnit(context.Background(), &un)
	return err
}

func (r *RPCRegistry) DestroyUnit(name string) error {
	fmt.Println("XRPCC", "DestroyUnit()", name)
	//return r.etcdRegistry.DestroyUnit(name)
	_, err := r.getClient().DestroyUnit(context.Background(), &rpc.UnitName{name})
	return err
}

func (r *RPCRegistry) UnitHeartbeat(name, machID string, ttl time.Duration) error {
	fmt.Println("XRPCC", "UnitHeartbeat()", name, machID)
	//return r.etcdRegistry.UnitHeartbeat(name, machID, ttl)
	_, err := r.getClient().UnitHeartbeat(context.Background(), &rpc.Heartbeat{
		Name:    name,
		Machine: machID,
		TTL:     int32(ttl.Seconds()),
	})
	return err
}

func (r *RPCRegistry) RemoveMachineState(machID string) error {
	return r.etcdRegistry.RemoveMachineState(machID)
}

func (r *RPCRegistry) RemoveUnitState(name string) error {
	fmt.Println("XRPCC", "RemoveUnitState()", name)
	//return r.etcdRegistry.RemoveUnitState(name)
	_, err := r.getClient().RemoveUnitState(context.Background(), &rpc.UnitName{name})
	return err
}

func (r *RPCRegistry) SaveUnitState(name string, unitState *unit.UnitState, ttl time.Duration) {
	//r.etcdRegistry.SaveUnitState(name, unitState, ttl)
	//return
	if unitState.UnitName == "" {
		unitState.UnitName = name
	}
	fmt.Println("XRPCC", "SaveUnitState()", name, unitState)

	r.getClient().SaveUnitState(context.Background(), &rpc.SaveUnitStateRequest{
		Name:  name,
		State: unitState.ToPB(),
		TTL:   int32(ttl.Seconds()),
	})
}

func (r *RPCRegistry) ScheduleUnit(name, machID string) error {
	fmt.Println("XRPCC", "ScheduleUnit()", name, machID)
	//return r.etcdRegistry.ScheduleUnit(name, machID)
	_, err := r.getClient().ScheduleUnit(context.Background(), &rpc.ScheduleUnitRequest{
		Name:    name,
		Machine: machID,
	})
	return err
}

func (r *RPCRegistry) SetUnitTargetState(name string, state job.JobState) error {
	fmt.Println("XRPCC", "SetUnitTargetState()", name, state)
	//return r.etcdRegistry.SetUnitTargetState(name, state)
	_, err := r.getClient().SetUnitTargetState(context.Background(), &rpc.ScheduledUnit{
		Name:         name,
		CurrentState: state.ToPB(),
	})
	return err
}

func (r *RPCRegistry) UnscheduleUnit(name, machID string) error {
	fmt.Println("XRPCC", "UnscheduleUnit()", name, machID)
	//return r.etcdRegistry.UnscheduleUnit(name, machID)
	_, err := r.getClient().UnscheduleUnit(context.Background(), &rpc.UnscheduleUnitRequest{
		Name:    name,
		Machine: machID,
	})
	return err
}

func (r *RPCRegistry) Machines() ([]machine.MachineState, error) {
	return r.etcdRegistry.Machines()
}

func (r *RPCRegistry) SetMachineState(ms machine.MachineState, ttl time.Duration) (uint64, error) {
	return r.etcdRegistry.SetMachineState(ms, ttl)
}

func (r *RPCRegistry) Schedule() ([]job.ScheduledUnit, error) {
	fmt.Println("XRPCC", "Schedule()")
	//return r.etcdRegistry.Schedule()
	scheduledUnits, err := r.getClient().GetScheduledUnits(context.Background(), &rpc.UnitFilter{})
	if err != nil {
		fmt.Println("ERROR XXX", err)
		return []job.ScheduledUnit{}, err
	}
	units := make([]job.ScheduledUnit, len(scheduledUnits.Units))

	for i, unit := range scheduledUnits.Units {
		state := rpcUnitStateToJobState(unit.CurrentState)
		units[i] = job.ScheduledUnit{
			Name:            unit.Name,
			TargetMachineID: unit.Machine,
			State:           &state,
		}
	}
	return units, err
}

func (r *RPCRegistry) ScheduledUnit(name string) (*job.ScheduledUnit, error) {
	fmt.Println("XRPCC", "ScheduleUnit()", name)
	//return r.etcdRegistry.ScheduledUnit(name)
	maybeSchedUnit, err := r.getClient().GetScheduledUnit(context.Background(), &rpc.UnitName{name})

	if err != nil {
		return nil, err
	}

	fmt.Println("XDATA CLIENT", "ScheduledUnit()", name, maybeSchedUnit)
	if scheduledUnit := maybeSchedUnit.GetUnit(); scheduledUnit != nil {
		state := rpcUnitStateToJobState(scheduledUnit.CurrentState)
		schedu := &job.ScheduledUnit{
			Name:            scheduledUnit.Name,
			TargetMachineID: scheduledUnit.Machine,
			State:           &state,
		}
		return schedu, err
	}
	return nil, nil

}

func (r *RPCRegistry) Unit(name string) (*job.Unit, error) {
	fmt.Println("XRPCC", "Unit()", name)
	//zzunits, zzerr := r.etcdRegistry.Unit(name)
	//fmt.Println("XRPCC Unit()", zzunits, zzerr)
	//zunits, zerr := r.getClient().GetUnit(context.Background(), &rpc.UnitName{name})
	//fmt.Println("XRPCC Unit()", zunits, zerr)

	//return r.etcdRegistry.Unit(name)
	maybeUnit, err := r.getClient().GetUnit(context.Background(), &rpc.UnitName{name})
	if err != nil {
		return nil, err
	}

	if unit := maybeUnit.GetUnit(); unit != nil {
		ur := rpcUnitToJobUnit(unit)
		fmt.Println("XRPCC Unit()", ur, err)
		//fmt.Println("XDATA CLIENT", "Unit()", ur)
		return ur, err
	}
	return nil, nil
}

func newCtx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(100)*time.Millisecond)
	return ctx
}

func (r *RPCRegistry) Units() ([]job.Unit, error) {
	fmt.Println("XRPCC", "Units()")
	if 1 == 2 {
		units, err := r.etcdRegistry.Units()
		//fmt.Println("XRPCY", units, err)
		//xunits, xerr := r.getClient().GetUnits(newCtx(), &rpc.UnitFilter{})
		//fmt.Println("XRPCR", xunits, xerr)
		return units, err
	}
	units, err := r.getClient().GetUnits(context.Background(), &rpc.UnitFilter{})
	if err != nil {
		fmt.Println("ERROR XXX", err)
		return []job.Unit{}, err
	}

	jobUnits := make([]job.Unit, len(units.Units))
	for i, u := range units.Units {
		jobUnit := rpcUnitToJobUnit(&u)
		jobUnits[i] = *jobUnit
	}
	return jobUnits, nil
}

func (r *RPCRegistry) UnitStates() ([]*unit.UnitState, error) {
	//return r.etcdRegistry.UnitStates()
	fmt.Println("XRPCC", "UnitStates()")

	unitStates, err := r.getClient().GetUnitStates(context.Background(), &rpc.UnitStateFilter{})
	if err != nil {
		return nil, err
	}

	nUnitStates := make([]*unit.UnitState, len(unitStates.UnitStates))

	for i, state := range unitStates.UnitStates {
		nUnitStates[i] = &unit.UnitState{
			UnitName:    state.Name,
			MachineID:   state.Machine,
			UnitHash:    state.Hash,
			LoadState:   state.LoadState,
			ActiveState: state.ActiveState,
			SubState:    state.SubState,
		}
		//fmt.Println("XENGINE", state, nUnitStates)
	}
	return nUnitStates, nil
}

func (r *RPCRegistry) EngineVersion() (int, error) {
	return r.etcdRegistry.EngineVersion()
}

func (r *RPCRegistry) UpdateEngineVersion(from, to int) error {
	return r.etcdRegistry.UpdateEngineVersion(from, to)
}

func (r *RPCRegistry) LatestDaemonVersion() (*semver.Version, error) {
	return r.etcdRegistry.LatestDaemonVersion()
}
