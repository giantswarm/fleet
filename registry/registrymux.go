package registry

import (
	"time"

	"github.com/coreos/fleet/Godeps/_workspace/src/github.com/coreos/go-semver/semver"

	"github.com/coreos/fleet/job"
	"github.com/coreos/fleet/machine"
	"github.com/coreos/fleet/unit"
)

type RegistryMux struct {
	etcdRegistry *EtcdRegistry
}

func (r *RegistryMux) getRegistry() Registry {
	return nil
}

func (r *RegistryMux) ClearUnitHeartbeat(name string) {
	r.getRegistry().ClearUnitHeartbeat(name)
}

func (r *RegistryMux) CreateUnit(unit *job.Unit) error {
	return r.getRegistry().CreateUnit(unit)
}

func (r *RegistryMux) DestroyUnit(unit string) error {
	return r.getRegistry().DestroyUnit(unit)
}

func (r *RegistryMux) UnitHeartbeat(name string, machID string, ttl time.Duration) error {
	return r.getRegistry().UnitHeartbeat(name, machID, ttl)
}

func (r *RegistryMux) Machines() ([]machine.MachineState, error) {
	return r.getRegistry().Machines()
}

func (r *RegistryMux) RemoveMachineState(machID string) error {
	return r.getRegistry().RemoveMachineState(machID)
}

func (r *RegistryMux) RemoveUnitState(jobName string) error {
	return r.getRegistry().RemoveUnitState(jobName)
}

func (r *RegistryMux) SaveUnitState(jobName string, unitState *unit.UnitState, ttl time.Duration) {
	r.getRegistry().SaveUnitState(jobName, unitState, ttl)
}

func (r *RegistryMux) ScheduleUnit(name string, machID string) error {
	return r.getRegistry().ScheduleUnit(name, machID)
}

func (r *RegistryMux) SetUnitTargetState(name string, state job.JobState) error {
	return r.getRegistry().SetUnitTargetState(name, state)
}

func (r *RegistryMux) SetMachineState(ms machine.MachineState, ttl time.Duration) (uint64, error) {
	return r.getRegistry().SetMachineState(ms, ttl)
}

func (r *RegistryMux) UnscheduleUnit(name string, machID string) error {
	return r.getRegistry().UnscheduleUnit(name, machID)
}

func (r *RegistryMux) Schedule() ([]job.ScheduledUnit, error) {
	return r.getRegistry().Schedule()
}

func (r *RegistryMux) ScheduledUnit(name string) (*job.ScheduledUnit, error) {
	return r.getRegistry().ScheduledUnit(name)
}

func (r *RegistryMux) Unit(name string) (*job.Unit, error) {
	return r.getRegistry().Unit(name)
}

func (r *RegistryMux) Units() ([]job.Unit, error) {
	return r.getRegistry().Units()
}

func (r *RegistryMux) UnitStates() ([]*unit.UnitState, error) {
	return r.getRegistry().UnitStates()
}

func (r *RegistryMux) LatestDaemonVersion() (*semver.Version, error) {
	return r.etcdRegistry.LatestDaemonVersion()
}

func (r *RegistryMux) EngineVersion() (int, error) {
	return r.etcdRegistry.EngineVersion()
}

func (r *RegistryMux) UpdateEngineVersion(from int, to int) error {
	return r.etcdRegistry.UpdateEngineVersion(from, to)
}
