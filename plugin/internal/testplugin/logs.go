package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	api "github.com/colony-2/pulse-provider-api"
)

// A real detached child writes its own stdout/stderr, closes both, then keeps
// progressing. File gates make source closure and admission independently visible.
func logWork() {
	dir, id := os.Args[2], os.Args[3]
	_ = os.WriteFile(filepath.Join(dir, "log-pid-"+id), []byte(fmt.Sprint(os.Getpid())), 0600)
	fmt.Fprint(os.Stdout, "stdout-before:", id, "\n")
	fmt.Fprint(os.Stderr, "stderr-before:", id, "\n")
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	released := false
	for {
		select {
		case <-deadline.C:
			return
		case <-tick.C:
		}
		if _, e := os.Stat(filepath.Join(dir, "log-exit")); e == nil {
			return
		}
		f, e := os.OpenFile(filepath.Join(dir, "log-progress-"+id), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e == nil {
			_, _ = f.Write([]byte("x"))
			_ = f.Close()
		}
		if !released {
			if _, e := os.Stat(filepath.Join(dir, "log-output")); e == nil {
				_, _ = os.Stdout.Write(bytes.Repeat([]byte{0xfe}, 4096))
				// More than the old cumulative diagnostic budget, with arbitrary bytes and
				// no reliance on newline framing. Pace writes through a fixture handshake.
				for i := 0; i < 300; i++ {
					_, _ = os.Stdout.Write(append([]byte{0xff, 0, 27}, []byte(fmt.Sprintf("[err]stdout-after:%s:%03d:%s\n", id, i, string(make([]byte, 256))))...))
					time.Sleep(time.Millisecond)
				}
				fmt.Fprint(os.Stderr, "stderr-after:", id, "\n")
				_ = os.Stdout.Close()
				_ = os.Stderr.Close()
				released = true
			}
		}
	}
}

type fixtureLogs struct {
	mu        sync.Mutex
	capture   sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	queue     chan api.LaunchLog
	coverage  []api.SourceStatus
	sequences map[string]uint64
	result    api.ListenerResult
	terminal  bool
	listener  api.ListenOptions
}

func newFixtureLogs(ls []api.Launch, l *api.ListenOptions, combined bool) *fixtureLogs {
	if l == nil {
		return nil
	}
	s := &fixtureLogs{done: make(chan struct{}), queue: make(chan api.LaunchLog, 64), sequences: map[string]uint64{}, listener: *l}
	s.ctx, s.cancel = context.WithCancel(l.Context)
	for _, v := range ls {
		for _, src := range []api.LogSource{api.LogSourceLaunch, api.LogSourceStdout, api.LogSourceStderr} {
			state := api.SourceActive
			var cause error
			if combined && src != api.LogSourceLaunch {
				state = api.SourceUnavailable
				cause = api.ErrLogSource
			}
			s.coverage = append(s.coverage, api.SourceStatus{LaunchID: v.LaunchID, Source: src, State: state, Cause: cause})
		}
		if combined {
			s.coverage = append(s.coverage, api.SourceStatus{LaunchID: v.LaunchID, Source: api.LogSourceCombined, State: api.SourceActive})
		}
	}
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				if errors.Is(l.Context.Err(), context.DeadlineExceeded) {
					s.finish(api.ListenerContextDeadline, context.DeadlineExceeded)
				} else {
					s.finish(api.ListenerContextCanceled, context.Canceled)
				}
				return
			case v, ok := <-s.queue:
				if !ok {
					s.finish(api.ListenerSourcesClosed, nil)
					return
				}
				s.mu.Lock()
				stopped := s.terminal
				if !stopped {
					for i := range s.coverage {
						q := &s.coverage[i]
						if q.LaunchID == v.LaunchID && q.Source == v.Source {
							q.LastSequence = v.Sequence
						}
					}
				}
				s.mu.Unlock()
				if stopped {
					return
				}
				if e := l.OnLog(s.ctx, v); e != nil {
					if errors.Is(e, api.ErrStopListening) {
						s.Stop()
					} else {
						s.finish(api.ListenerError, api.ErrLogDelivery)
					}
					return
				}
			}
		}
	}()
	if l.Duration > 0 {
		timer := time.AfterFunc(l.Duration, func() { s.finish(api.ListenerDurationElapsed, nil) })
		go func() { <-s.done; timer.Stop() }()
	}
	return s
}
func (s *fixtureLogs) emit(id string, src api.LogSource, data []byte) {
	if s == nil {
		return
	}
	s.capture.Lock()
	defer s.capture.Unlock()
	for len(data) > 0 {
		n := min(len(data), api.MaxLogRecordBytes)
		s.sequences[id]++
		v := api.LaunchLog{LaunchID: id, SubscriptionID: "fixture-logs", Sequence: s.sequences[id], Source: src, Timestamp: time.Now(), Data: append([]byte(nil), data[:n]...)}
		data = data[n:]
		select {
		case <-s.done:
			return
		case s.queue <- v:
		default:
			s.finish(api.ListenerOverflow, api.ErrLogCapacity)
			return
		}
	}
}
func (s *fixtureLogs) sourceClosed(id string, src api.LogSource) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.coverage {
		q := &s.coverage[i]
		if q.LaunchID == id && q.Source == src {
			q.State = api.SourceClosed
			q.Closed = true
		}
	}
}
func (s *fixtureLogs) finish(reason api.ListenerReason, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return
	}
	s.terminal = true
	for i := range s.coverage {
		q := &s.coverage[i]
		if q.State == api.SourceActive || q.State == api.SourcePending {
			q.State = api.SourceInterrupted
		}
	}
	s.result = api.ListenerResult{Reason: reason, Cause: cause, Coverage: append([]api.SourceStatus(nil), s.coverage...)}
	s.cancel()
	close(s.done)
}
func (s *fixtureLogs) Stop()                 { s.finish(api.ListenerStopped, nil) }
func (s *fixtureLogs) Done() <-chan struct{} { return s.done }
func (s *fixtureLogs) Coverage() []api.SourceStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]api.SourceStatus(nil), s.coverage...)
}
func (s *fixtureLogs) Wait(ctx context.Context) (api.ListenerResult, error) {
	select {
	case <-s.done:
	case <-ctx.Done():
		select {
		case <-s.done:
		default:
			return api.ListenerResult{}, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.result
	r.Coverage = append([]api.SourceStatus(nil), r.Coverage...)
	return r, r.Cause
}
func (f *fixture) submitLogs(ctx context.Context, ls []api.Launch, o api.SubmitOptions) ([]api.Submission, api.LogSubscription, error) {
	if o.Listener != nil && (o.Listener.Context == nil || o.Listener.OnLog == nil || o.Listener.Duration < 0) {
		return nil, nil, errors.New("invalid listener")
	}
	s := newFixtureLogs(ls, o.Listener, f.mode == "logs-combined")
	var handle api.LogSubscription
	if s != nil {
		handle = s
	}
	if f.mode == "logs-error" {
		out := make([]api.Submission, len(ls))
		for i, l := range ls {
			appendEvent(f.dir, "submit "+l.LaunchID)
			s.emit(l.LaunchID, api.LogSourceLaunch, []byte("launch-error:"+l.LaunchID))
			out[i] = api.Submission{LaunchID: l.LaunchID, Status: api.Rejected, Reason: "fixture launch failure"}
			if s != nil {
				s.sourceClosed(l.LaunchID, api.LogSourceLaunch)
			}
		}
		if s != nil {
			s.mu.Lock()
			for i := range s.coverage {
				q := &s.coverage[i]
				if q.Source != api.LogSourceLaunch {
					q.State = api.SourceUnavailable
					q.Cause = api.ErrLogSource
				}
			}
			s.mu.Unlock()
			close(s.queue)
		}
		return out, handle, errors.New("fixture launch failure")
	}
	if f.mode == "logs-none" && s != nil {
		s.mu.Lock()
		for i := range s.coverage {
			s.coverage[i].State = api.SourceUnavailable
			s.coverage[i].Cause = api.ErrLogSource
		}
		s.mu.Unlock()
		s.finish(api.ListenerSourceUnavailable, api.ErrLogSource)
	}
	var readers sync.WaitGroup
	for _, l := range ls {
		appendEvent(f.dir, "submit "+l.LaunchID)
		s.emit(l.LaunchID, api.LogSourceLaunch, []byte("launch-before:"+l.LaunchID+"\n"))
		exe, _ := os.Executable()
		cmd := exec.Command(exe, "log-work", f.dir, l.LaunchID)
		cmd.Env = []string{}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		var stdout, stderr io.ReadCloser
		var combinedWrite *os.File
		if f.mode == "logs-combined" {
			r, w, e := os.Pipe()
			if e != nil {
				return nil, handle, e
			}
			stdout = r
			combinedWrite = w
			cmd.Stdout = w
			cmd.Stderr = w
		} else {
			stdout, _ = cmd.StdoutPipe()
			stderr, _ = cmd.StderrPipe()
		}
		if e := cmd.Start(); e != nil {
			if combinedWrite != nil {
				_ = combinedWrite.Close()
			}
			if stdout != nil {
				_ = stdout.Close()
			}
			if stderr != nil {
				_ = stderr.Close()
			}
			if s != nil {
				s.finish(api.ListenerSourceError, api.ErrLogSource)
			}
			return nil, handle, e
		}
		if combinedWrite != nil {
			_ = combinedWrite.Close()
		}
		drain := func(r io.ReadCloser, src api.LogSource) {
			defer readers.Done()
			defer r.Close()
			buffer := make([]byte, api.MaxLogRecordBytes)
			for {
				n, e := r.Read(buffer)
				if n > 0 {
					s.emit(l.LaunchID, src, buffer[:n])
				}
				if e != nil {
					break
				}
			}
			s.sourceClosed(l.LaunchID, src)
		}
		if f.mode == "logs-combined" {
			readers.Add(1)
			// A single pipe genuinely merges both child execution streams.
			go drain(stdout, api.LogSourceCombined)
		} else {
			readers.Add(2)
			go drain(stdout, api.LogSourceStdout)
			go drain(stderr, api.LogSourceStderr)
		}
		go cmd.Wait()
	}
	if f.mode == "logs-hold" {
		waitFile(ctx, filepath.Join(f.dir, "log-admit"))
	}
	for _, l := range ls {
		s.emit(l.LaunchID, api.LogSourceLaunch, []byte("launch-after:"+l.LaunchID+"\n"))
		s.sourceClosed(l.LaunchID, api.LogSourceLaunch)
	}
	if s != nil {
		go func() { readers.Wait(); close(s.queue) }()
	}
	out := make([]api.Submission, len(ls))
	for i, l := range ls {
		out[i] = api.Submission{LaunchID: l.LaunchID, Status: api.Accepted}
	}
	return out, handle, nil
}
