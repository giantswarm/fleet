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

package unit

import (
	"reflect"
	"testing"
)

func assertGenerateUnitStateHeartbeats(t *testing.T, um UnitManager, gen *UnitStateGenerator, expect *UnitStateHeartbeats) {
	beatchan, err := gen.Generate()
	if err != nil {
		t.Fatalf("Unexpected error from Generate(): %v", err)
	}

	var got *UnitStateHeartbeats
	for beat := range beatchan {
		got = beat
	}

	if !reflect.DeepEqual(got, expect) {
		t.Fatalf("got %#v, expected %#v", got, expect)
	}
}

func TestUnitStateGeneratorSubscribeLifecycle(t *testing.T) {
	um := NewFakeUnitManager()
	um.Load("foo.service", UnitFile{})

	gen := NewUnitStateGenerator(um)

	empty := &UnitStateHeartbeats{
		States: make([]*UnitStateHeartbeat, 0),
	}
	// not subscribed to anything yet, so no heartbeats
	assertGenerateUnitStateHeartbeats(t, um, gen, empty)

	gen.Subscribe("foo.service")

	// subscribed to foo.service so we should get a heartbeat
	expect := &UnitStateHeartbeats{
		States: []*UnitStateHeartbeat{
			{Name: "foo.service", State: &UnitState{"loaded", "active", "running", "", "", "foo.service"}},
		},
	}
	assertGenerateUnitStateHeartbeats(t, um, gen, expect)

	gen.Unsubscribe("foo.service")

	// heartbeat for foo.service should have nil State since we have not called Generate since Unsubscribe
	expect = &UnitStateHeartbeats{
		States: []*UnitStateHeartbeat{
			{Name: "foo.service", State: nil},
		},
	}
	assertGenerateUnitStateHeartbeats(t, um, gen, expect)

	// since the nil-State heartbeat for foo.service was sent for the last Generate, it can be forgotten
	assertGenerateUnitStateHeartbeats(t, um, gen, empty)
}

func TestUnitStateGeneratorNoState(t *testing.T) {
	um := NewFakeUnitManager()
	gen := NewUnitStateGenerator(um)

	empty := &UnitStateHeartbeats{
		States: make([]*UnitStateHeartbeat, 0),
	}
	// not subscribed to anything yet, so no heartbeats
	assertGenerateUnitStateHeartbeats(t, um, gen, empty)

	gen.Subscribe("foo.service")

	// subscribed to foo.service but no underlying state so no heartbeat
	assertGenerateUnitStateHeartbeats(t, um, gen, empty)
}
