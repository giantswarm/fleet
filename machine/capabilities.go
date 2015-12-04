package machine

const (
	CapGRPC     = "GRPC"
	CapNOENGINE = "!ENGINE"
)

type Capabilities map[string]bool

func (c Capabilities) Has(capability string) bool {
	if c == nil {
		return false
	}
	if has, exists := c[capability]; exists {
		return has
	}
	return false
}
