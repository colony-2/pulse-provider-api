package plugin

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestUnauthenticatedTransportAndInvocationFence(t *testing.T) {
	o := options(t, "")
	p := start(t, o)
	g := p.current
	addr := g.client.ReattachConfig().Addr
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	// Trust the server certificate for this test, but supply no client identity.
	conn, e := grpc.NewClient("passthrough:///unauthenticated", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, addr.Network(), addr.String())
	}), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})), grpc.WithDisableRetry())
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	if _, e = wire.NewProviderClient(conn).GetCapabilities(ctx, &wire.Empty{}); e == nil {
		t.Fatal("unauthenticated request succeeded")
	}
	if count(o.Init.BaseDir, "capabilities") != 0 {
		t.Fatal("unauthenticated handler invoked")
	}
	l, _ := toLaunch(launches("fenced")[0])
	req := &wire.SubmitRequest{Invocation: 10000, Launches: []*wire.Launch{l}}
	if _, e = g.rpc.Submit(context.Background(), req); e != nil {
		t.Fatal(e)
	}
	if _, e = g.rpc.Submit(context.Background(), req); e == nil {
		t.Fatal("duplicate invocation accepted")
	}
	req.Invocation = 1
	if _, e = g.rpc.Submit(context.Background(), req); e == nil {
		t.Fatal("retired invocation accepted")
	}
	if count(o.Init.BaseDir, "submit fenced") != 1 {
		t.Fatal(events(o.Init.BaseDir))
	}
	t.Log("handler counts: unauthenticated=0; duplicate and retired invocation tokens collectively execute once")
}
func TestUnsupportedSemanticsBeforeFactory(t *testing.T) {
	original := semantics
	defer func() { semantics = original }()
	semantics = append(append([]string(nil), original...), "future-required-constraint")
	o := options(t, "")
	p, e := Start(context.Background(), o)
	if e == nil || p != nil || count(o.Init.BaseDir, "factory ") != 0 {
		t.Fatal("unsupported semantics reached factory", e, events(o.Init.BaseDir))
	}
}

func TestFrameworkHealthAndProcessMetadataExcludePayloads(t *testing.T) {
	o := options(t, "mixed")
	o.Init.ConfigYAML = []byte("mode: fixture\n")
	p := start(t, o)
	_, _, _, _ = p.SubmitDetailed(context.Background(), launches("private-launch"), api.SubmitOptions{})
	h := fmt.Sprintf("%+v", p.Health())
	for _, secret := range []string{"exact-lease", "private-method-error", "private-launch", "mode: fixture", "executor-default"} {
		if strings.Contains(h, secret) {
			t.Fatal("health leaks private data")
		}
	}
	pid := readPID(t, o.Init.BaseDir, "pid")
	for _, file := range []string{"cmdline", "environ"} {
		data, e := os.ReadFile(fmt.Sprintf("/proc/%d/%s", pid, file))
		if e != nil {
			t.Fatal(e)
		}
		for _, secret := range []string{"exact-lease", "private-launch", "private-method-error", "executor-default", "mode: fixture"} {
			if strings.Contains(string(data), secret) {
				t.Fatal("process metadata leaks private data", file)
			}
		}
	}
	// Framework failures contain fixed codes, while returned provider method errors
	// remain explicitly private output belonging to the calling host.
	bad := o
	bad.Init.Name = "wrong-name"
	_, e := Start(context.Background(), bad)
	if e == nil || strings.Contains(e.Error(), "sentinel private initialization") {
		t.Fatal("factory diagnostics leak", e)
	}
}
