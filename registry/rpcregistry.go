package registry

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/coreos/fleet/Godeps/_workspace/src/github.com/coreos/go-semver/semver"
	"github.com/coreos/fleet/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/fleet/Godeps/_workspace/src/google.golang.org/grpc"

	"github.com/coreos/fleet/debug"
	"github.com/coreos/fleet/job"
	"github.com/coreos/fleet/machine"
	"github.com/coreos/fleet/pkg"
	"github.com/coreos/fleet/rpc"
	"github.com/coreos/fleet/unit"
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

	connectMu *sync.RWMutex

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
		connectMu:            new(sync.RWMutex),
		currentLeader:        "",
	}
}

func (r *RPCRegistry) NewEventStream() pkg.EventStream {
	return r
}

func (r *RPCRegistry) Next(stop chan struct{}) chan pkg.Event {
	defer debug.Exit_(debug.Enter_("SIGNAL"))
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
		fmt.Println("SIGNAL RECEIVED")
		ch <- "newEvent"
	}
}

func (r *RPCRegistry) ctx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	return ctx
}

func (r *RPCRegistry) Start() {
	for leaderUpdate := range r.leaderUpdateNotifier {
		if r.currentLeader == leaderUpdate {
			continue
		}
		if r.currentLeader == r.localMachine.String() {
			r.listener.Close()
			r.done <- struct{}{}
		}
		r.currentLeader = leaderUpdate
		fmt.Println("LEAD XXX got new leader", leaderUpdate)
		if r.currentLeader == r.localMachine.String() {
			fmt.Println("LEAD XXX local machine is leader, doing things ")
			r.startServer()
		}
		if r.registryConn != nil {
			r.registryConn.Close()
		}
		r.connect()

		// go func() {
		// 	for {
		// 		eventsStream, err := r.getClient().AgentEvents(r.ctx(), &rpc.MachineProperties{r.localMachine.String()})
		// 		if err != nil {
		// 			continue
		// 		}
		// 		for {
		// 			events, err := eventsStream.Recv()
		// 			if err == io.EOF {
		// 				break
		// 			}
		// 			if err != nil {
		// 				break
		// 			}
		// 			r.broadcastEvent(agentEvent{events.UnitIds})
		// 		}
		// 	}
		// }()

	}
}

func (r *RPCRegistry) dialer(addr string, timeout time.Duration) (net.Conn, error) {
	for {
		addr := fmt.Sprintf("%s:%d", r.findMachineAddr(r.currentLeader), port)
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			return conn, nil
		}
		time.Sleep(time.Millisecond * 200)
	}
}

func (r *RPCRegistry) connect() {
	timeout := 500 * time.Millisecond
	//r.connectMu.Lock()
	addr := fmt.Sprintf("%s:%d", r.findMachineAddr(r.currentLeader), port)
	var err error
	r.registryConn, err = grpc.Dial(addr, grpc.WithInsecure(), grpc.WithDialer(r.dialer), grpc.WithTimeout(timeout), grpc.WithBlock())
	if err != nil {
		fmt.Println("XXX FAILURE", err)
		log.Fatalf("unable to connect to registry: %s", err)
	}

	r.registryClient = rpc.NewRegistryClient(r.registryConn)
	//r.connectMu.Unlock()
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
	// r.connectMu.RLock()
	// defer r.connectMu.RUnlock()
	for ; ; time.Sleep(100 * time.Millisecond) {
		if r.registryClient != nil {
			break
		}
	}
	return r.registryClient
}

func (r *RPCRegistry) ClearUnitHeartbeat(name string) {
	defer debug.Exit_(debug.Enter_(name))

	r.getClient().ClearUnitHeartbeat(r.ctx(), &rpc.UnitName{name})
}

func (r *RPCRegistry) CreateUnit(j *job.Unit) error {
	defer debug.Exit_(debug.Enter_(j.Name))

	un := j.ToPB()
	_, err := r.getClient().CreateUnit(r.ctx(), &un)
	return err
}

func (r *RPCRegistry) DestroyUnit(name string) error {
	defer debug.Exit_(debug.Enter_(name))

	_, err := r.getClient().DestroyUnit(r.ctx(), &rpc.UnitName{name})
	return err
}

func (r *RPCRegistry) UnitHeartbeat(name, machID string, ttl time.Duration) error {
	defer debug.Exit_(debug.Enter_(name, machID))

	_, err := r.getClient().UnitHeartbeat(r.ctx(), &rpc.Heartbeat{
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
	_, err := r.getClient().RemoveUnitState(r.ctx(), &rpc.UnitName{name})
	return err
}

func (r *RPCRegistry) SaveUnitState(name string, unitState *unit.UnitState, ttl time.Duration) {
	defer debug.Exit_(debug.Enter_(name, unitState))

	if unitState.UnitName == "" {
		unitState.UnitName = name
	}

	r.getClient().SaveUnitState(r.ctx(), &rpc.SaveUnitStateRequest{
		Name:  name,
		State: unitState.ToPB(),
		TTL:   int32(ttl.Seconds()),
	})
}

func (r *RPCRegistry) ScheduleUnit(name, machID string) error {
	defer debug.Exit_(debug.Enter_(name, machID))

	_, err := r.getClient().ScheduleUnit(r.ctx(), &rpc.ScheduleUnitRequest{
		Name:    name,
		Machine: machID,
	})
	return err
}

func (r *RPCRegistry) SetUnitTargetState(name string, state job.JobState) error {
	defer debug.Exit_(debug.Enter_(name, state))

	_, err := r.getClient().SetUnitTargetState(r.ctx(), &rpc.ScheduledUnit{
		Name:         name,
		CurrentState: state.ToPB(),
	})
	return err
}

func (r *RPCRegistry) UnscheduleUnit(name, machID string) error {
	defer debug.Exit_(debug.Enter_(name, machID))

	_, err := r.getClient().UnscheduleUnit(r.ctx(), &rpc.UnscheduleUnitRequest{
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
	defer debug.Exit_(debug.Enter_())

	scheduledUnits, err := r.getClient().GetScheduledUnits(r.ctx(), &rpc.UnitFilter{})
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
	defer debug.Exit_(debug.Enter_(name))

	maybeSchedUnit, err := r.getClient().GetScheduledUnit(r.ctx(), &rpc.UnitName{name})

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
	defer debug.Exit_(debug.Enter_(name))

	maybeUnit, err := r.getClient().GetUnit(r.ctx(), &rpc.UnitName{name})
	if err != nil {
		return nil, err
	}

	if unit := maybeUnit.GetUnit(); unit != nil {
		ur := rpcUnitToJobUnit(unit)
		return ur, nil
	}
	return nil, nil
}

func (r *RPCRegistry) Units() ([]job.Unit, error) {
	defer debug.Exit_(debug.Enter_())

	units, err := r.getClient().GetUnits(r.ctx(), &rpc.UnitFilter{})
	if err != nil {
		//TODO XXX ERROR me
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
	defer debug.Exit_(debug.Enter_())

	unitStates, err := r.getClient().GetUnitStates(r.ctx(), &rpc.UnitStateFilter{})
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
