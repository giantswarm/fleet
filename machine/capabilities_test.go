package machine

import "testing"

func TestCapabilities(t *testing.T) {
	machineState := &MachineState{Capabilities: Capabilities{CapDISABLE_ENGINE: true, CapGRPC: true}}

	if !machineState.Capabilities.Has("GRPC") {
		t.Errorf("Unexpected Capability GRPC %s", machineState.Capabilities["GRPC"])
	}

	if !machineState.Capabilities.Has("DISABLE_ENGINE") {
		t.Errorf("Unexpected Capability DISABLE_ENGINE %s", machineState.Capabilities["DISABLE_ENGINE"])
	}
}
