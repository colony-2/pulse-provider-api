package plugin

import (
	"context"

	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	hp "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

var handshake = hp.HandshakeConfig{ProtocolVersion: Protocol, MagicCookieKey: "PULSE_PLUGIN_COOKIE", MagicCookieValue: "pulse-provider-api-plugin"}
var semantics = []string{"exact-leases", "integer-resources", "start-before", "native-runtime-cap", "method-errors", "per-launch-outcomes", "opaque-cursors", "no-replay"}

type transportPlugin struct {
	hp.NetRPCUnsupportedPlugin
	server *server
}

func (p *transportPlugin) GRPCServer(_ *hp.GRPCBroker, s *grpc.Server) error {
	wire.RegisterProviderServer(s, p.server)
	return nil
}
func (p *transportPlugin) GRPCClient(_ context.Context, _ *hp.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return wire.NewProviderClient(c), nil
}
