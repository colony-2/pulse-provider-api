package plugin

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func command(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	cmd.Env = append(append(os.Environ(), "GOWORK=off"), env...)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("%v: %v\n%s", args, e, out)
	}
	return string(out)
}
func TestIndependentConsumersAndExamples(t *testing.T) {
	root, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	module := `module example.org/consumer

go 1.22

require (
 github.com/colony-2/pulse-provider-api v0.0.0-20261105011626-401f11e595a8
 github.com/colony-2/pulse-provider-api/plugin v0.0.0
)
`
	// This isolated candidate validation uses a temporary replacement. The release
	// check below intentionally uses a fresh cache and no replacement at all.
	if e = os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0600); e != nil {
		t.Fatal(e)
	}
	command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "mod", "edit", "-replace=github.com/colony-2/pulse-provider-api/plugin="+root)
	command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "mod", "edit", "-replace=github.com/colony-2/pulse-provider-api="+filepath.Dir(root))
	for _, name := range []string{"provider", "host", "reverse-host", "reverse-provider"} {
		source, e := os.ReadFile(filepath.Join("examples", name, "main.go"))
		if e != nil {
			t.Fatal(e)
		}
		target := filepath.Join(dir, name)
		if e = os.Mkdir(target, 0700); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(target, "main.go"), source, 0600); e != nil {
			t.Fatal(e)
		}
	}
	command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "mod", "tidy")
	command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "build", "-o", filepath.Join(dir, "provider-bin"), "./provider")
	command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "build", "-o", filepath.Join(dir, "reverse-host-bin"), "./reverse-host")
	command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "build", "-o", filepath.Join(dir, "reverse-provider-bin"), "./reverse-provider")
	helper, e := os.ReadFile(filepath.Join("scripts", "reverse-consumer.go"))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "reverse-consumer.go"), helper, 0600); e != nil {
		t.Fatal(e)
	}
	output := command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "run", "reverse-consumer.go", filepath.Join(dir, "reverse-host-bin"), filepath.Join(dir, "reverse-provider-bin"))
	if !strings.Contains(output, "independent reverse collector join/leave verification passed") {
		t.Fatal(output)
	}
	if !strings.Contains(output, "independent callback baseline, ordered deltas and stopped delivery passed") {
		t.Fatal(output)
	}
	t.Log(output)
	command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "build", "-o", filepath.Join(dir, "host-bin"), "./host")
	output = command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, filepath.Join(dir, "host-bin"), filepath.Join(dir, "provider-bin"))
	if !strings.Contains(output, "ready protocol=3 bases=1") {
		t.Fatal(output)
	}
	// Compile the detached fixture and an independent listener against BOTH local
	// candidate modules, then exercise process and authenticated network delivery.
	fixtureDir := filepath.Join(dir, "fixture")
	if e = os.Mkdir(fixtureDir, 0700); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"main.go", "logs.go"} {
		b, e := os.ReadFile(filepath.Join("internal", "testplugin", name))
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(fixtureDir, name), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "build", "-o", filepath.Join(dir, "fixture-bin"), "./fixture")
	listenerDir := filepath.Join(dir, "listener")
	if e = os.Mkdir(listenerDir, 0700); e != nil {
		t.Fatal(e)
	}
	source, e := os.ReadFile(filepath.Join("testdata", "listener_consumer_test.go"))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(listenerDir, "listener_test.go"), source, 0600); e != nil {
		t.Fatal(e)
	}
	trust := testTrust(t, "fixture").files(t)
	t.Log(command(t, dir, []string{"GOTOOLCHAIN=go1.22.12", "LOG_FIXTURE=" + filepath.Join(dir, "fixture-bin"), "LOG_TRUST=" + trust}, "go", "test", "-v", "./listener"))
	t.Log(command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "list", "-m", "-json", "github.com/colony-2/pulse-provider-api", "github.com/colony-2/pulse-provider-api/plugin"))

	deps := command(t, dir, []string{"GOTOOLCHAIN=go1.22.12"}, "go", "list", "-deps", "./...")
	for _, forbidden := range []string{"github.com/colony-2/pulse/", "github.com/colony-2/c2j/", "jobdb"} {
		if strings.Contains(deps, forbidden) {
			t.Fatal("unexpected dependency", forbidden)
		}
	}
	t.Log("independent copied author and host build and run: ready protocol=3 bases=1; temporary candidate replacement only")
}
func TestAPIOnlyGo122Consumer(t *testing.T) {
	dir := t.TempDir()
	if e := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.org/api-only\n\ngo 1.22\n\nrequire github.com/colony-2/pulse-provider-api v0.0.0-20261105011626-401f11e595a8\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "api_test.go"), []byte("package consumer\nimport (\"testing\"; api \"github.com/colony-2/pulse-provider-api\")\nfunc TestAPI(t *testing.T){if api.MaxBatch!=100{t.Fatal(api.MaxBatch)}}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	env := []string{"GOTOOLCHAIN=go1.22.12"}
	root, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	command(t, dir, env, "go", "mod", "edit", "-replace=github.com/colony-2/pulse-provider-api="+filepath.Dir(root))
	command(t, dir, env, "go", "mod", "tidy")
	command(t, dir, env, "go", "test", "./...")
	modules := command(t, dir, env, "go", "list", "-m", "all")
	if len(strings.Split(strings.TrimSpace(modules), "\n")) != 2 {
		t.Fatal(modules)
	}
	deps := command(t, dir, env, "go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/colony-2/pulse-provider-api")
	if strings.TrimSpace(deps) != "github.com/colony-2/pulse-provider-api" {
		t.Fatal(deps)
	}
	t.Log("candidate root API compiled under Go 1.22.12; no nonstandard dependency except the API module")
}
