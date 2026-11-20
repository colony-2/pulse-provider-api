package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin"
)

type invocationCollector struct {
	plugin.ProviderCollector
	snapshots []plugin.ProviderSnapshot
}

func (c *invocationCollector) Snapshot() (plugin.ProviderSnapshot, error) {
	snapshot := c.snapshots[0]
	c.snapshots = c.snapshots[1:]
	return snapshot, nil
}

type discoveryProvider struct {
	api.Provider
	err error
}

func (p *discoveryProvider) GetCapabilities(context.Context) (api.Capabilities, error) {
	// Complete, empty data alongside an error must not establish absence.
	return api.Capabilities{BasesComplete: true}, p.err
}

func TestInvocationRejectsFailedDiscoveryAndRecovers(t *testing.T) {
	healthy := &discoveryProvider{}
	departing := &discoveryProvider{err: errors.New("provider departed after snapshot")}
	snapshot := plugin.ProviderSnapshot{Revision: 3, Providers: []plugin.AvailableProvider{
		{ID: "alpha", Generation: 1, Provider: healthy},
		{ID: "beta", Generation: 2, Provider: departing},
	}}
	result, err := invocation(context.Background(), snapshot)
	if !errors.Is(err, departing.err) || result != "" {
		t.Fatalf("failed discovery produced membership: %q, %v", result, err)
	}
	// The next invocation uses a fresh snapshot and can still use survivors.
	snapshot.Revision++
	snapshot.Providers = snapshot.Providers[:1]
	result, err = invocation(context.Background(), snapshot)
	if err != nil || result != "revision=4 providers=alpha/1" {
		t.Fatalf("survivor invocation: %q, %v", result, err)
	}
	// The same complete, empty capabilities are a successful read when no
	// method error accompanies them; provider membership still comes from Snapshot.
	departing.err = nil
	snapshot.Providers = append(snapshot.Providers, plugin.AvailableProvider{ID: "beta", Generation: 3, Provider: departing})
	result, err = invocation(context.Background(), snapshot)
	if err != nil || result != "revision=4 providers=alpha/1,beta/3" {
		t.Fatalf("successful discovery control: %q, %v", result, err)
	}
	result, err = invocation(context.Background(), plugin.ProviderSnapshot{Revision: 5})
	if err != nil || result != "revision=5 providers=" {
		t.Fatalf("empty membership: %q, %v", result, err)
	}
}

func TestHostContinuesAfterProviderDeparture(t *testing.T) {
	healthy := &discoveryProvider{}
	departed := &discoveryProvider{err: errors.New("provider departed after snapshot")}
	source := &invocationCollector{snapshots: []plugin.ProviderSnapshot{
		{Revision: 3, Providers: []plugin.AvailableProvider{
			{ID: "alpha", Generation: 1, Provider: healthy},
			{ID: "beta", Generation: 2, Provider: departed},
		}},
		{Revision: 4, Providers: []plugin.AvailableProvider{{ID: "alpha", Generation: 1, Provider: healthy}}},
		{Revision: 5},
	}}
	lines := make(chan string, 4)
	for _, line := range []string{"invoke", "invoke", "invoke", "quit"} {
		lines <- line
	}
	var output bytes.Buffer
	if err := serveInvocations(source, lines, nil, &output); err != nil {
		t.Fatalf("provider departure ended collection: %v", err)
	}
	want := "invocation failed\nrevision=4 providers=alpha/1\nrevision=5 providers=\n"
	if output.String() != want || len(source.snapshots) != 0 {
		t.Fatalf("failed read or subsequent membership mishandled: %q", output.String())
	}
}

func TestHostSupervisesSubscriptionFailure(t *testing.T) {
	stopped := make(chan error, 1)
	stopped <- plugin.ErrCallback
	var output bytes.Buffer
	if e := serveInvocations(&invocationCollector{}, nil, stopped, &output); !errors.Is(e, plugin.ErrCallback) {
		t.Fatal(e)
	}
	line := changeLine(plugin.ProviderChange{Kind: plugin.ProviderChangeMembership, Revision: 4, Reason: plugin.ProviderReasonConnectionLost, Left: []plugin.AvailableProvider{{ID: "alpha", Generation: 1}}})
	if line != "change kind=membership revision=4 reason=connection_lost joined= left=alpha/1" {
		t.Fatal(line)
	}
}

func TestHostKeyAndCommandsShareInput(t *testing.T) {
	input, writer, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer input.Close()
	defer writer.Close()
	_, _ = io.WriteString(writer, " disposable key \ninvoke\nquit\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var out bytes.Buffer
	if e = runHost(ctx, []string{"-auth", "mutual-key", "-transport", "http", "-auth-key-stdin", "-auth-key-id", "alpha"}, input, &out); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "revision=1 providers=\n") || strings.Contains(out.String(), "disposable") {
		t.Fatal(out.String())
	}
}
func TestHostKeyInputStopsBeforeListening(t *testing.T) {
	for _, kind := range []string{"cancel", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			input, writer, e := os.Pipe()
			if e != nil {
				t.Fatal(e)
			}
			defer input.Close()
			defer writer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if kind == "cancel" {
				cancel()
			}
			// A premature Listen would fail with a bind error rather than the input error.
			reserved, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer reserved.Close()
			var out bytes.Buffer
			if e = runHost(ctx, []string{"-listen", reserved.Addr().String(), "-auth", "mutual-key", "-transport", "http", "-auth-key-stdin", "-auth-key-id", "alpha"}, input, &out); e == nil || !strings.Contains(e.Error(), "key input unavailable") || out.Len() != 0 {
				t.Fatal(e, out.String())
			}
			// An interrupted acquisition finished reading, and cleared its deadline.
			_, _ = io.WriteString(writer, "next\n")
			line, e := bufio.NewReader(input).ReadString('\n')
			if e != nil || line != "next\n" {
				t.Fatal(line, e)
			}
		})
	}
}
func TestHostInvalidKeyFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-auth", "unknown"}, {"-transport", "http"},
		{"-auth", "mutual-key", "-transport", "http", "-auth-key-id", "alpha"},
		{"-auth", "mutual-key", "-transport", "http", "-auth-key-stdin"},
		{"-auth", "mutual-key", "-transport", "http", "-auth-key-id", "alpha", "-auth-key-stdin", "-auth-key-env", "KEY"},
		{"-auth", "mutual-key", "-transport", "http", "-auth-key-id", "alpha", "-auth-key-stdin", "-identities", "alpha"},
		{"-auth-key-env", "KEY"},
	} {
		_, _, e := hostOptions(context.Background(), args, nil, nil)
		if !errors.Is(e, plugin.ErrInvalid) {
			t.Fatal(args, e)
		}
	}
}
