package plugin

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These fixtures execute the real publication script against disposable local
// repositories. Only the independent consumer is stubbed: this is control-flow
// evidence, never proof that a published module resolves or credentials work.
type releaseFixture struct {
	t            *testing.T
	work, remote string
	env          []string
}

func newReleaseFixture(t *testing.T) *releaseFixture {
	t.Helper()
	dir := t.TempDir()
	f := &releaseFixture{t: t, work: filepath.Join(dir, "work"), remote: filepath.Join(dir, "remote.git")}
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "GIT_") {
			f.env = append(f.env, entry)
		}
	}
	f.env = append(f.env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	if err := os.Mkdir(f.work, 0700); err != nil {
		t.Fatal(err)
	}
	f.git("init", "--bare", "--initial-branch=main", f.remote)
	f.git("init", "--initial-branch=main")
	f.git("remote", "add", "origin", f.remote)
	script, err := os.ReadFile("scripts/publish.sh")
	if err != nil {
		t.Fatal(err)
	}
	f.write("plugin/scripts/publish.sh", string(script))
	f.write("plugin/scripts/check-release.sh", "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$1\" >> release-checks\n[ ! -f fail-release-check ]\n")
	f.write("plugin/VERSION", "v0.1.0\n")
	f.write("plugin/contents", "original\n")
	f.commit()
	f.git("push", "origin", "HEAD:refs/heads/main")
	return f
}

func (f *releaseFixture) write(name, value string) {
	f.t.Helper()
	path := filepath.Join(f.work, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0700); err != nil {
		f.t.Fatal(err)
	}
}

func (f *releaseFixture) git(args ...string) string {
	f.t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=Release fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
	cmd.Dir, cmd.Env = f.work, f.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (f *releaseFixture) commit() string {
	f.t.Helper()
	f.git("add", "plugin")
	f.git("commit", "-m", "fixture candidate")
	return f.git("rev-parse", "HEAD")
}

func (f *releaseFixture) publish(hash, branch string) (string, error) {
	f.t.Helper()
	cmd := exec.Command("sh", "plugin/scripts/publish.sh", hash, branch)
	cmd.Dir, cmd.Env = f.work, f.env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (f *releaseFixture) assertNoRelease() {
	f.t.Helper()
	if tags := f.git("--git-dir", f.remote, "tag", "--list"); tags != "" {
		f.t.Fatal("unexpected tag:", tags)
	}
	f.assertChecks(0)
}

func (f *releaseFixture) assertChecks(want int) {
	f.t.Helper()
	data, err := os.ReadFile(filepath.Join(f.work, "release-checks"))
	if err != nil && !os.IsNotExist(err) {
		f.t.Fatal(err)
	}
	if string(data) != strings.Repeat("v0.1.0\n", want) {
		f.t.Fatalf("independent-check calls: %q; want %d", data, want)
	}
}

func TestReleaseScriptNativeBranch(t *testing.T) {
	f := newReleaseFixture(t)
	hash := f.git("rev-parse", "HEAD")
	for calls := 1; calls <= 2; calls++ {
		if out, err := f.publish(hash, "refs/heads/main"); err != nil {
			t.Fatalf("native branch: %v\n%s", err, out)
		}
		if got := f.git("--git-dir", f.remote, "rev-parse", "refs/tags/plugin/v0.1.0"); got != hash {
			t.Fatal("release tag differs from managed commit:", got, hash)
		}
		f.assertChecks(calls)
	}
	t.Log("native refs/heads/main: local tag matches managed hash; check calls=2 including existing-tag retry")
}

func TestReleaseScriptHashSafeguards(t *testing.T) {
	for _, mismatch := range []string{"HEAD", "remote branch"} {
		t.Run(mismatch, func(t *testing.T) {
			f := newReleaseFixture(t)
			old := f.git("rev-parse", "HEAD")
			f.write("plugin/contents", "changed\n")
			current := f.commit()
			message := "published branch must match the release candidate"
			if mismatch == "HEAD" {
				current = old
				message = "release workspace differs from published candidate"
			}
			if out, err := f.publish(current, "refs/heads/main"); err == nil || !strings.Contains(out, message) {
				t.Fatal("expected hash safeguard:", err, out)
			}
			f.assertNoRelease()
			t.Log("hash mismatch: tag writes=0 independent-check calls=0")
		})
	}
}

func TestReleaseScriptConflictingTag(t *testing.T) {
	f := newReleaseFixture(t)
	old := f.git("rev-parse", "HEAD")
	f.git("push", "origin", "HEAD:refs/tags/plugin/v0.1.0")
	f.write("plugin/contents", "conflicting module\n")
	current := f.commit()
	f.git("push", "origin", "HEAD:refs/heads/main")
	if out, err := f.publish(current, "refs/heads/main"); err == nil || !strings.Contains(out, "plugin version already identifies different contents") {
		t.Fatal("expected conflicting-version safeguard:", err, out)
	}
	if got := f.git("--git-dir", f.remote, "rev-parse", "refs/tags/plugin/v0.1.0"); got != old {
		t.Fatal("existing tag moved:", got, old)
	}
	f.assertChecks(0)
	t.Log("conflicting module: existing tag preserved; independent-check calls=0")
}

func TestReleaseScriptConsumerFailure(t *testing.T) {
	f := newReleaseFixture(t)
	hash := f.git("rev-parse", "HEAD")
	f.write("fail-release-check", "")
	if out, err := f.publish(hash, "refs/heads/main"); err == nil || strings.Contains(out, "independent no-replacement author/host verification passed") {
		t.Fatal("failed consumer reported success:", err, out)
	}
	f.assertChecks(1)
	if got := f.git("--git-dir", f.remote, "rev-parse", "refs/tags/plugin/v0.1.0"); got != hash {
		t.Fatal("completed tag effect lost:", got, hash)
	}
	t.Log("consumer failure: nonzero result; local tag retained; independent-check calls=1")
}

func TestReleaseScriptRejectsUnsupportedBranchRefs(t *testing.T) {
	f := newReleaseFixture(t)
	hash := f.git("rev-parse", "HEAD")
	for _, branch := range []string{"main", "refs/tags/main", "refs/heads/*", "refs/heads/../main"} {
		if out, err := f.publish(hash, branch); err == nil {
			t.Fatalf("unsupported branch %q accepted: %s", branch, out)
		}
		f.assertNoRelease()
	}
}
