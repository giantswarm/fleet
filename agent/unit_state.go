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
	"reflect"
	"sync"
	"time"

	"github.com/coreos/fleet/Godeps/_workspace/src/github.com/jonboulle/clockwork"

	"github.com/coreos/fleet/log"
	"github.com/coreos/fleet/machine"
	"github.com/coreos/fleet/registry"
	"github.com/coreos/fleet/unit"
	pb "github.com/coreos/fleet/protobuf"

)


func NewUnitStatePublisher(reg registry.Registry, mach machine.Machine, ttl time.Duration) *UnitStatePublisher {
	return &UnitStatePublisher{
		mach:            mach,
		ttl:             ttl,
		publisher:       newPublisher(reg, ttl),
		cache:           make(map[string]*unit.UnitState),
		cacheMutex:      sync.RWMutex{},
		toPublish:       make(chan map[string]*unit.UnitState),
		clock:           clockwork.NewRealClock(),
	}
}

type publishFunc func(unitStates map[string]*unit.UnitState)

type UnitStatePublisher struct {
	mach machine.Machine
	ttl  time.Duration

	cache      map[string]*unit.UnitState
	cacheMutex sync.RWMutex

	// toPublish is a queue indicating unit names for which a state publish event should occur.
	// It is possible for a unit name to end up in the queue for which a
	// state has already been published, in which case it triggers a no-op.
	toPublish chan map[string]*unit.UnitState

	publisher publishFunc

	clock clockwork.Clock
}

// Run caches all of the heartbeat objects from the provided channel, publishing
// them to the Registry every 5s. Heartbeat objects are also published as they
// are received on the channel.
func (p *UnitStatePublisher) Run(beatchan <-chan *unit.UnitStateHeartbeats, stop <-chan struct{}) {
	var period time.Duration
	if p.ttl > 10*time.Second {
		period = p.ttl * 4 / 5
	} else {
		period = p.ttl / 2
	}

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-p.clock.After(period):
				if len(p.cache) > 0 {
					go p.publisher(p.cache)

					go func() {
						p.cacheMutex.Lock()
						for name, state := range p.cache {
							if state == nil {
								log.Debugf("Cleaning content of unit hearbeat cache of %s", name)
								delete(p.cache, name)
							}
						}
						p.cacheMutex.Unlock()
					}()
				}
			case units := <-p.toPublish:
					if len(units) > 0 {
						go p.publisher(units)
						p.clock.Now()
					}
			}
		}
	}()

	machID := p.mach.State().ID

	for {
		select {
		case <-stop:
			return
		case hearbeat := <-beatchan:
			publishCache := false
			for _, bt := range hearbeat.States {
				if bt.State != nil {
					bt.State.MachineID = machID
				}

				if p.updateCache(bt) {
					publishCache = true
				}
			}

			if publishCache {
				p.toPublish <- p.cache
			}
		}
	}
}

func (p *UnitStatePublisher) MarshalJSON() ([]byte, error) {
	p.cacheMutex.Lock()
	data := struct {
		Cache     map[string]*unit.UnitState
	}{
		Cache:     p.cache,
	}
	p.cacheMutex.Unlock()

	return json.Marshal(data)
}

func (p *UnitStatePublisher) pruneCache() {
	for name, us := range p.cache {
		if us == nil {
			delete(p.cache, name)
		}
	}
}

// updateCache updates the cache of UnitStates which the UnitStatePublisher
// uses to determine when a change has occurred, and to do a periodic
// publishing of all UnitStates. It returns a boolean indicating whether the
// state in the given UnitStateHeartbeat differs from the state from the
// previous heartbeat of this unit, if any exists.
func (p *UnitStatePublisher) updateCache(update *unit.UnitStateHeartbeat) (changed bool) {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	last, ok := p.cache[update.Name]
	p.cache[update.Name] = update.State

	if !ok || !reflect.DeepEqual(last, update.State) {
		changed = true
	}
	return
}

// Purge ensures that the UnitStates for all Units known in the
// UnitStatePublisher's cache are removed from the registry.
func (p *UnitStatePublisher) Purge() {
	for name := range p.cache {
		p.cache[name] = nil
	}
	p.publisher(p.cache)
}

// newPublisher returns a publishFunc that publishes a single UnitState
// by the given name to the provided Registry, with the given TTL
func newPublisher(reg registry.Registry, ttl time.Duration) publishFunc {
	return func(unitStates map[string]*unit.UnitState) {
		states := make([]*pb.SaveUnitStateRequest, 0)
		for name, state := range unitStates {
			if state == nil {
				log.Debugf("Destroying UnitState(%s) in Registry", name)
				err := reg.RemoveUnitState(name)
				if err != nil {
					log.Errorf("Failed to destroy UnitState(%s) in Registry: %v", name, err)
				}
			} else {
				// Sanity check - don't want to publish incomplete UnitStates
				// TODO(jonboulle): consider teasing apart a separate UnitState-like struct
				// so we can rely on a UnitState always being fully hydrated?

				// See https://github.com/coreos/fleet/issues/720
				//if len(us.UnitHash) == 0
				//	log.Errorf("Refusing to push UnitState(%s), no UnitHash: %#v", name, us)
				if len(state.MachineID) == 0 {
					log.Errorf("Refusing to push UnitState(%s), no MachineID: %#v", name, state)
				} else {
					log.Debugf("Pushing UnitState(%s) to Registry: %#v", name, state)
					unitState := &pb.SaveUnitStateRequest{
						Name: name,
						State: state.ToPB(),
						TTL: int32(ttl.Seconds()),
					}
					states = append(states, unitState)
				}
			}
		}
		if len(states) > 0 {
			log.Debugf("Save %d unit states using unit publisher", len(states))
			reg.SaveUnitStates(states)
		}

	}
}
