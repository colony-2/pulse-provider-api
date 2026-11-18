package plugin

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
)

// A runner owns exactly one SDK process, never its native workload descendants.
type processRunner struct {
	cmd                             *exec.Cmd
	out, errOut, outWrite, errWrite *os.File
	done                            chan struct{}
	waitErr                         error
	pid                             int
	startup                         context.Context
	stop                            func() bool
}

func newRunner(cmd *exec.Cmd, startup context.Context) (*processRunner, error) {
	r := &processRunner{cmd: cmd, startup: startup, done: make(chan struct{})}
	var e error
	r.out, r.outWrite, e = os.Pipe()
	if e != nil {
		return nil, e
	}
	r.errOut, r.errWrite, e = os.Pipe()
	if e != nil {
		r.out.Close()
		r.outWrite.Close()
		return nil, e
	}
	cmd.Stdout = r.outWrite
	cmd.Stderr = r.errWrite
	cmd.Stdin = nil
	return r, nil
}
func (r *processRunner) Start(context.Context) error {
	if e := r.startup.Err(); e != nil {
		r.waitErr = e
		r.outWrite.Close()
		r.errWrite.Close()
		close(r.done)
		return e
	}
	e := r.cmd.Start()
	r.outWrite.Close()
	r.errWrite.Close()
	if e != nil {
		r.waitErr = e
		close(r.done)
		return e
	}
	r.pid = r.cmd.Process.Pid
	r.stop = context.AfterFunc(r.startup, func() { _ = r.cmd.Process.Kill() })
	go func() { r.waitErr = r.cmd.Wait(); close(r.done) }()
	return nil
}
func (r *processRunner) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		return r.waitErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *processRunner) Kill(context.Context) error {
	if r.cmd.Process != nil {
		e := r.cmd.Process.Kill()
		if !errors.Is(e, os.ErrProcessDone) {
			return e
		}
	}
	return nil
}
func (r *processRunner) Stdout() io.ReadCloser { return r.out }
func (r *processRunner) Stderr() io.ReadCloser { return r.errOut }
func (r *processRunner) Name() string          { return "provider" }
func (r *processRunner) ID() string {
	if r.pid == 0 {
		return ""
	}
	return strconv.Itoa(r.pid)
}
func (r *processRunner) Diagnose(context.Context) string                  { return "plugin startup failed" }
func (r *processRunner) PluginToHost(n, a string) (string, string, error) { return n, a, nil }
func (r *processRunner) HostToPlugin(n, a string) (string, string, error) { return n, a, nil }
func (r *processRunner) closePipes()                                      { r.out.Close(); r.errOut.Close() }
