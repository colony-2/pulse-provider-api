package plugin

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func byteStrings(in []string) [][]byte {
	out := make([][]byte, len(in))
	for i, v := range in {
		out[i] = []byte(v)
	}
	return out
}
func stringsBytes(in [][]byte) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}
func toJob(j api.JobRef) *wire.JobRef {
	return &wire.JobRef{Jobdb: []byte(j.JobDB), InstanceId: []byte(j.InstanceID), TenantId: []byte(j.TenantID), JobId: []byte(j.JobID)}
}
func fromJob(j *wire.JobRef) api.JobRef {
	return api.JobRef{JobDB: string(j.GetJobdb()), InstanceID: string(j.GetInstanceId()), TenantID: string(j.GetTenantId()), JobID: string(j.GetJobId())}
}
func toTime(t *time.Time) (*timestamppb.Timestamp, error) {
	if t == nil {
		return nil, nil
	}
	p := timestamppb.New(*t)
	return p, p.CheckValid()
}
func fromTime(t *timestamppb.Timestamp) (*time.Time, error) {
	if t == nil {
		return nil, nil
	}
	if err := t.CheckValid(); err != nil {
		return nil, ErrInvalid
	}
	v := t.AsTime()
	return &v, nil
}
func toLaunch(l api.Launch) (*wire.Launch, error) {
	t, err := toTime(l.StartBefore)
	if err != nil {
		return nil, ErrInvalid
	}
	return &wire.Launch{LaunchId: []byte(l.LaunchID), Allocation: &wire.Allocation{CpuMillis: l.CPUMillis, MemoryBytes: l.MemoryBytes, ScratchBytes: l.ScratchBytes, Base: []byte(l.Base), Capabilities: byteStrings(l.Capabilities)}, Job: toJob(l.Job), Lease: []byte(l.Lease), TimeoutSeconds: l.TimeoutSeconds, StartBefore: t}, nil
}
func fromLaunch(l *wire.Launch) (api.Launch, error) {
	if l == nil || l.Allocation == nil || l.Job == nil {
		return api.Launch{}, ErrInvalid
	}
	t, err := fromTime(l.StartBefore)
	if err != nil {
		return api.Launch{}, err
	}
	a := l.Allocation
	return api.Launch{LaunchID: string(l.LaunchId), Allocation: api.Allocation{CPUMillis: a.CpuMillis, MemoryBytes: a.MemoryBytes, ScratchBytes: a.ScratchBytes, Base: string(a.Base), Capabilities: stringsBytes(a.Capabilities)}, Job: fromJob(l.Job), Lease: api.Lease(l.Lease), TimeoutSeconds: l.TimeoutSeconds, StartBefore: t}, nil
}
func toSub(s api.Submission) *wire.Submission {
	return &wire.Submission{LaunchId: []byte(s.LaunchID), Status: []byte(s.Status), Reason: []byte(s.Reason)}
}
func fromSub(s *wire.Submission) api.Submission {
	return api.Submission{LaunchID: string(s.GetLaunchId()), Status: api.Status(s.GetStatus()), Reason: string(s.GetReason())}
}
func toCaps(c api.Capabilities) *wire.CapabilitiesResponse {
	w := &wire.CapabilitiesResponse{BasesComplete: c.BasesComplete}
	for _, b := range c.Bases {
		w.Bases = append(w.Bases, &wire.BaseCapabilities{Name: []byte(b.Name), Capabilities: byteStrings(b.Capabilities)})
	}
	return w
}
func fromCaps(c *wire.CapabilitiesResponse) api.Capabilities {
	out := api.Capabilities{BasesComplete: c.BasesComplete, Bases: make([]api.BaseCapabilities, 0, len(c.Bases))}
	for _, b := range c.Bases {
		out.Bases = append(out.Bases, api.BaseCapabilities{Name: string(b.GetName()), Capabilities: stringsBytes(b.GetCapabilities())})
	}
	return out
}
func toList(c api.ListResponse) (*wire.ListResponse, error) {
	w := &wire.ListResponse{NextPageToken: []byte(c.NextPageToken)}
	for _, i := range c.Items {
		ct, e := toTime(i.CreatedAt)
		if e != nil {
			return nil, ErrInvalid
		}
		st, e := toTime(i.StartedAt)
		if e != nil {
			return nil, ErrInvalid
		}
		w.Items = append(w.Items, &wire.Instance{Id: []byte(i.ID), LaunchId: []byte(i.LaunchID), State: []byte(i.State), Job: toJob(i.Job), Refs: byteStrings(i.Refs), CreatedAt: ct, StartedAt: st})
	}
	return w, nil
}
func fromList(w *wire.ListResponse) (api.ListResponse, error) {
	c := api.ListResponse{Items: make([]api.Instance, 0, len(w.Items)), NextPageToken: string(w.NextPageToken)}
	for _, i := range w.Items {
		if i == nil || i.Job == nil {
			return api.ListResponse{}, ErrInvalid
		}
		ct, e := fromTime(i.CreatedAt)
		if e != nil {
			return api.ListResponse{}, e
		}
		st, e := fromTime(i.StartedAt)
		if e != nil {
			return api.ListResponse{}, e
		}
		c.Items = append(c.Items, api.Instance{ID: string(i.Id), LaunchID: string(i.LaunchId), State: string(i.State), Job: fromJob(i.Job), Refs: stringsBytes(i.Refs), CreatedAt: ct, StartedAt: st})
	}
	return c, nil
}

type methodError struct{ text, class string }

func (e *methodError) Error() string { return e.text }
func (e *methodError) Is(target error) bool {
	return (e.class == "canceled" && target == context.Canceled) || (e.class == "deadline" && target == context.DeadlineExceeded)
}
func toError(e error) *wire.MethodError {
	if e == nil {
		return nil
	}
	c := "method"
	if errors.Is(e, context.Canceled) {
		c = "canceled"
	}
	if errors.Is(e, context.DeadlineExceeded) {
		c = "deadline"
	}
	return &wire.MethodError{Text: []byte(e.Error()), Class: c}
}
func fromError(e *wire.MethodError) error {
	if e == nil {
		return nil
	}
	return &methodError{text: string(e.Text), class: e.Class}
}

var launchID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func validateLaunches(ls []api.Launch) error {
	if len(ls) < 1 || len(ls) > api.MaxBatch {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, l := range ls {
		if !launchID.MatchString(l.LaunchID) || seen[l.LaunchID] || l.Base == "" || l.Lease == "" || l.Job.JobDB == "" || l.Job.InstanceID == "" || l.Job.TenantID == "" || l.Job.JobID == "" || l.TimeoutSeconds < 0 || l.TimeoutSeconds > 31536000 {
			return ErrInvalid
		}
		seen[l.LaunchID] = true
		for _, v := range []int64{l.CPUMillis, l.MemoryBytes, l.ScratchBytes} {
			if v < 0 || v > api.MaxQuantity {
				return ErrInvalid
			}
		}
		if _, e := toTime(l.StartBefore); e != nil {
			return ErrInvalid
		}
	}
	return nil
}
func validateList(r *api.ListRequest) error {
	if r.PageSize == 0 {
		r.PageSize = 100
	}
	if r.PageSize < 1 || r.PageSize > 100 || len(r.PageToken) > 16384 {
		return ErrInvalid
	}
	return nil
}
func validSub(s api.Submission) bool {
	switch s.Status {
	case api.Accepted:
		return s.Reason == ""
	case api.NoCapacity, api.Unsupported, api.Unavailable, api.Rejected, api.Unknown:
		return s.Reason != ""
	}
	return false
}
func normalize(ls []api.Launch, ss []api.Submission) ([]api.Submission, bool) {
	by := map[string][]api.Submission{}
	for _, s := range ss {
		by[s.LaunchID] = append(by[s.LaunchID], s)
	}
	out := make([]api.Submission, len(ls))
	bad := false
	for i, l := range ls {
		v := by[l.LaunchID]
		if len(v) == 1 && validSub(v[0]) {
			out[i] = v[0]
		} else {
			bad = true
			out[i] = api.Submission{LaunchID: l.LaunchID, Status: api.Unknown, Reason: "missing or malformed submission result"}
		}
	}
	return out, bad
}
func nonStart(ls []api.Launch, status api.Status, e error) []api.Submission {
	out := make([]api.Submission, len(ls))
	for i, l := range ls {
		out[i] = api.Submission{LaunchID: l.LaunchID, Status: status, Reason: e.Error()}
	}
	return out
}
func observations(id, gen uint64, ss []api.Submission, e *wire.MethodError) SubmissionObservations {
	o := SubmissionObservations{Invocation: id, Generation: gen, Capture: "complete", Outcomes: make([]OutcomeObservation, 0)}
	left := MaxObservationBytes
	cut := func(s string) (string, bool) {
		if len(s) <= left {
			left -= len(s)
			return s, false
		}
		s = s[:left]
		left = 0
		o.Capture = "truncated"
		return s, true
	}
	if e != nil {
		o.MethodError.Present = true
		o.MethodError.Class, o.MethodError.Truncated = cut(e.Class)
		var truncated bool
		o.MethodError.Text, truncated = cut(string(e.Text))
		o.MethodError.Truncated = o.MethodError.Truncated || truncated
	}
	for _, s := range ss {
		if left == 0 || len(o.Outcomes) >= MaxObservationEntries {
			o.Capture = "truncated"
			break
		}
		i, it := cut(s.LaunchID)
		st, stt := cut(string(s.Status))
		r, rt := cut(s.Reason)
		o.Outcomes = append(o.Outcomes, OutcomeObservation{LaunchID: i, Status: st, Reason: r, Truncated: it || stt || rt})
	}
	return o
}
func malformedError(bad bool, e error) error {
	if bad {
		return errors.Join(e, fmt.Errorf("plugin: malformed submission outcomes"))
	}
	return e
}
