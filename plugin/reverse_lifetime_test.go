package plugin

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

func reverseProcess(t *testing.T, address, trust, dir, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(fixtureBinary, "reverse", address, trust)
	cmd.Env = []string{"EVENT_DIR=" + dir, "MODE=" + mode}
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	return cmd
}
func TestReverseNativeWorkSurvivesSessionAndProviderLoss(t *testing.T) {
	for _, mode := range []string{"native", "nativedie"} {
		t.Run(mode, func(t *testing.T) {
			tr := testTrust(t, "fixture")
			trust := tr.files(t)
			c := testCollector(t, collectorOptions(tr))
			dir := t.TempDir()
			cmd := reverseProcess(t, c.listener.Addr().String(), trust, dir, mode)
			handle := nextSnapshot(t, c, 1).Providers[0].Provider
			ss, _, e := handle.Submit(context.Background(), launches("reverse-native"), api.SubmitOptions{})
			if mode == "native" {
				if e != nil || ss[0].Status != api.Accepted {
					t.Fatal(ss, e)
				}
				observer := callbackSubscribe(t, c, func(context.Context, ProviderChange) { panic("private observer failure") })
				if e = callbackWait(t, observer); !errors.Is(e, ErrCallback) {
					t.Fatal(e)
				}
				old := handle.(*Connection)
				if e = old.Close(); e != nil {
					t.Fatal(e)
				}
				nextSnapshot(t, c, 0)
				replacement := reverseProcess(t, c.listener.Addr().String(), trust, dir, "")
				newer := nextSnapshot(t, c, 1).Providers[0]
				if newer.Generation <= old.g.id {
					t.Fatal("reconnect reused generation")
				}
				if _, e = old.GetCapabilities(context.Background()); !errors.Is(e, ErrUnavailable) {
					t.Fatal(e)
				}
				// Check progress after reconnection before race-instrumented process
				// exit delays consume the native cap. Then check cap expiry and reaping.
				verifyNativeCap(t, dir)
				if e = c.Close(); e != nil {
					t.Fatal(e)
				}
				_ = replacement.Wait()
			} else {
				if e == nil || ss[0].Status != api.Unknown {
					t.Fatal(ss, e)
				}
				_ = cmd.Wait()
				verifyNativeCap(t, dir)
			}
			_ = cmd.Wait()
			if count(dir, "submit reverse-native") != 1 {
				t.Fatal(events(dir))
			}
		})
	}
}
func TestReverseAbruptHostLossSparesNativeWork(t *testing.T) {
	tr := testTrust(t, "fixture")
	trust := tr.files(t)
	dir := t.TempDir()
	host := exec.Command(fixtureBinary, "reverse-host", dir, trust)
	host.Env = []string{}
	if e := host.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = host.Process.Kill(); _ = host.Wait() }()
	var address string
	eventually(t, func() bool {
		b, e := os.ReadFile(filepath.Join(dir, "host-address"))
		address = string(b)
		return e == nil
	})
	cmd := reverseProcess(t, address, trust, dir, "native")
	eventually(t, func() bool { _, e := os.Stat(filepath.Join(dir, "host-ready")); return e == nil })
	_ = host.Process.Kill()
	_ = host.Wait()
	_ = cmd.Wait()
	waitEvent(t, dir, "maintenance_canceled")
	verifyNativeCap(t, dir)
	if count(dir, "cleanup") != 1 || count(dir, "submit host-loss") != 1 {
		t.Fatal(events(dir))
	}
}
func TestReverseHungCallbacksEndRun(t *testing.T) {
	for _, mode := range []string{"hungfactory", "hungcleanup"} {
		t.Run(mode, func(t *testing.T) {
			tr := testTrust(t, "fixture")
			trust := tr.files(t)
			o := collectorOptions(tr)
			o.StartupTimeout = 100 * time.Millisecond
			c := testCollector(t, o)
			dir := t.TempDir()
			cmd := reverseProcess(t, c.listener.Addr().String(), trust, dir, mode)
			waitEvent(t, dir, "factory "+mode)
			if mode == "hungcleanup" {
				nextSnapshot(t, c, 1)
				_ = c.Close()
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			select {
			case e := <-exited:
				if e == nil {
					t.Fatal("hung callback reported success")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Run did not return to supervisor")
			}
			if strings.Count(events(dir), "factory "+mode) != 1 {
				t.Fatal("hung factory replaced")
			}
		})
	}
}
