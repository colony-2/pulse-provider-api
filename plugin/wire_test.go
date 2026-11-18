package plugin

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	api "github.com/colony-2/pulse-provider-api"
	"github.com/colony-2/pulse-provider-api/plugin/internal/wire"
	"google.golang.org/protobuf/proto"
)

func TestEveryAPIFieldAndExactRoundTrip(t *testing.T) {
	// Inventory both the pinned module's compiled API and the repository root AST.
	inventory := map[string][]string{
		"Capabilities": {"Bases", "BasesComplete"}, "BaseCapabilities": {"Name", "Capabilities"}, "Allocation": {"CPUMillis", "MemoryBytes", "ScratchBytes", "Base", "Capabilities"}, "JobRef": {"JobDB", "InstanceID", "TenantID", "JobID"}, "Launch": {"LaunchID", "Allocation", "Job", "Lease", "TimeoutSeconds", "StartBefore"}, "Submission": {"LaunchID", "Status", "Reason"}, "ListRequest": {"PageSize", "PageToken", "LaunchID"}, "ListResponse": {"Items", "NextPageToken"}, "Instance": {"ID", "LaunchID", "State", "Job", "Refs", "CreatedAt", "StartedAt"},
	}
	for _, v := range []any{api.Capabilities{}, api.BaseCapabilities{}, api.Allocation{}, api.JobRef{}, api.Launch{}, api.Submission{}, api.ListRequest{}, api.ListResponse{}, api.Instance{}} {
		typ := reflect.TypeOf(v)
		var fields []string
		for i := 0; i < typ.NumField(); i++ {
			fields = append(fields, typ.Field(i).Name)
		}
		if !reflect.DeepEqual(fields, inventory[typ.Name()]) {
			t.Fatalf("pinned API mapping changed: %s %v", typ.Name(), fields)
		}
	}
	root, e := parser.ParseFile(token.NewFileSet(), "../provider.go", nil, 0)
	if e != nil {
		t.Fatal(e)
	}
	ast.Inspect(root, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}
		want, ok := inventory[ts.Name.Name]
		if !ok {
			return false
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			t.Fatal("API struct removed")
		}
		var fields []string
		for _, f := range st.Fields.List {
			if len(f.Names) == 0 {
				fields = append(fields, f.Type.(*ast.Ident).Name)
			} else {
				for _, name := range f.Names {
					fields = append(fields, name.Name)
				}
			}
		}
		if !reflect.DeepEqual(fields, want) {
			t.Fatalf("root API changed: %s %v", ts.Name, fields)
		}
		delete(inventory, ts.Name.Name)
		return false
	})
	if len(inventory) != 0 {
		t.Fatal("missing root types", inventory)
	}
	l := launches("roundtrip")[0]
	l.MemoryBytes = api.MaxQuantity
	l.ScratchBytes = 12345
	l.Capabilities = []string{"GPU", "gpu", "gpu", "network"}
	l.Job = api.JobRef{JobDB: "https://jobdb/tenant\xff", InstanceID: "instance\xfe", TenantID: "tenant", JobID: "job"}
	l.TimeoutSeconds = 31536000
	for _, stamp := range []*time.Time{nil, timePointer(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)), timePointer(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC))} {
		l.StartBefore = stamp
		w, e := toLaunch(l)
		if e != nil {
			t.Fatal(e)
		}
		b, e := proto.Marshal(w)
		if e != nil {
			t.Fatal(e)
		}
		v := new(wire.Launch)
		if e = proto.Unmarshal(b, v); e != nil {
			t.Fatal(e)
		}
		got, e := fromLaunch(v)
		if e != nil || !reflect.DeepEqual(got, l) {
			t.Fatalf("launch field loss\ngot %#v\nwant %#v %v", got, l, e)
		}
	}
	caps := api.Capabilities{Bases: []api.BaseCapabilities{{Name: "warming/arm", Capabilities: []string{"GPU", "network"}}, {Name: "busy", Capabilities: []string{}}}, BasesComplete: true}
	if !reflect.DeepEqual(caps, fromCaps(toCaps(caps))) {
		t.Fatal("capabilities loss")
	}
	caps.BasesComplete = false
	if fromCaps(toCaps(caps)).BasesComplete {
		t.Fatal("partial flag lost")
	}
	ts := time.Date(2026, 1, 1, 1, 2, 3, 123456789, time.UTC)
	listing := api.ListResponse{Items: []api.Instance{{ID: "instance-one", LaunchID: "launch", State: "running", Job: l.Job, Refs: []string{"native\xff", "second"}, CreatedAt: &ts, StartedAt: &ts}, {ID: "instance-two", LaunchID: "launch", State: "queued", Job: l.Job, Refs: []string{}}}, NextPageToken: "\xff\xfe\x00opaque"}
	w, e := toList(listing)
	if e != nil {
		t.Fatal(e)
	}
	b, e := proto.Marshal(w)
	if e != nil {
		t.Fatal(e)
	}
	v := new(wire.ListResponse)
	if e = proto.Unmarshal(b, v); e != nil {
		t.Fatal(e)
	}
	got, e := fromList(v)
	if e != nil || !reflect.DeepEqual(got, listing) {
		t.Fatalf("listing loss %#v %v", got, e)
	}
	empty, e := fromList(&wire.ListResponse{NextPageToken: []byte("\xff")})
	if e != nil || empty.Items == nil || empty.NextPageToken != "\xff" {
		t.Fatal(empty, e)
	}
}
func timePointer(v time.Time) *time.Time { return &v }
func TestValidationBoundaries(t *testing.T) {
	ls := launches("valid")
	ls[0].CPUMillis = 0
	ls[0].MemoryBytes = api.MaxQuantity
	if e := validateLaunches(ls); e != nil {
		t.Fatal(e)
	}
	for _, v := range []int64{-1, api.MaxQuantity + 1, 1<<63 - 1} {
		l := launches("bad")
		l[0].MemoryBytes = v
		if validateLaunches(l) == nil {
			t.Fatal("quantity accepted", v)
		}
	}
	for _, year := range []int{0, 10000} {
		l := launches("bad")
		l[0].StartBefore = timePointer(time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC))
		if validateLaunches(l) == nil {
			t.Fatal("timestamp accepted", year)
		}
	}
	for _, n := range []int64{-1, 31536001} {
		l := launches("bad")
		l[0].TimeoutSeconds = n
		if validateLaunches(l) == nil {
			t.Fatal("cap accepted", n)
		}
	}
	if validateLaunches(append(launches("a"), launches("a")...)) == nil {
		t.Fatal("duplicate IDs accepted")
	}
	for _, size := range []int{-1, 101} {
		q := api.ListRequest{PageSize: size}
		if validateList(&q) == nil {
			t.Fatal(size)
		}
	}
	q := api.ListRequest{}
	if validateList(&q) != nil || q.PageSize != 100 {
		t.Fatal(q)
	}
	q.PageToken = strings.Repeat("a", 16385)
	if validateList(&q) == nil {
		t.Fatal("token bound")
	}
}
func TestMethodErrorPresenceAndClassification(t *testing.T) {
	for _, e := range []error{nil, errors.New(""), context.Canceled, context.DeadlineExceeded, errors.Join(errors.New("original"), context.DeadlineExceeded)} {
		w := toError(e)
		data, marshalErr := proto.Marshal(&wire.SubmitResponse{Error: w})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		envelope := new(wire.SubmitResponse)
		if marshalErr = proto.Unmarshal(data, envelope); marshalErr != nil {
			t.Fatal(marshalErr)
		}
		got := fromError(envelope.Error)
		if (got == nil) != (e == nil) {
			t.Fatal("presence lost")
		}
		if e != nil && got.Error() != e.Error() {
			t.Fatal("text lost")
		}
		for _, target := range []error{context.Canceled, context.DeadlineExceeded} {
			if errors.Is(got, target) != errors.Is(e, target) {
				t.Fatal("classification lost")
			}
		}
	}
}
func TestNormalizeIndependentOutcomes(t *testing.T) {
	ls := launches("accepted", "declined", "missing", "duplicate", "future", "reason")
	ss := []api.Submission{{LaunchID: "accepted", Status: api.Accepted}, {LaunchID: "declined", Status: api.Unavailable, Reason: "not ready"}, {LaunchID: "duplicate", Status: api.Accepted}, {LaunchID: "duplicate", Status: api.Accepted}, {LaunchID: "future", Status: api.Status("new-status"), Reason: "arbitrary"}, {LaunchID: "reason", Status: api.Accepted, Reason: "bad"}, {LaunchID: "unrequested", Status: api.Accepted}}
	got, bad := normalize(ls, ss)
	if !bad || got[0].Status != api.Accepted || got[1].Status != api.Unavailable {
		t.Fatal(got)
	}
	for _, s := range got[2:] {
		if s.Status != api.Unknown {
			t.Fatal(s)
		}
	}
	for _, status := range []api.Status{api.NoCapacity, api.Unsupported, api.Unavailable, api.Rejected, api.Unknown} {
		if validSub(api.Submission{Status: status}) {
			t.Fatal("empty decline reason")
		}
	}
}
func TestBoundedObservations(t *testing.T) {
	var ss []api.Submission
	for i := 0; i < 1000; i++ {
		ss = append(ss, api.Submission{LaunchID: "a", Status: api.Rejected, Reason: strings.Repeat("r", 1000)})
	}
	e := &wire.MethodError{Text: []byte(strings.Repeat("e", MaxObservationBytes*2)), Class: "method"}
	o := observations(1, 2, ss, e)
	if o.Capture != "truncated" || !o.MethodError.Truncated || o.Invocation != 1 || o.Generation != 2 {
		t.Fatal(o)
	}
	o = observations(2, 3, ss, nil)
	n := 0
	for _, v := range o.Outcomes {
		n += len(v.LaunchID) + len(v.Status) + len(v.Reason)
	}
	if n > MaxObservationBytes || len(o.Outcomes) > MaxObservationEntries || o.Capture != "truncated" {
		t.Fatal(n, len(o.Outcomes))
	}
	if ss[0].Reason != strings.Repeat("r", 1000) {
		t.Fatal("authoritative mutation")
	}
	zero := make([]api.Submission, 10000)
	if len(observations(3, 4, zero, nil).Outcomes) > MaxObservationEntries {
		t.Fatal("empty observations unbounded")
	}
}
func TestStrictConfigurationAndPaths(t *testing.T) {
	type config struct {
		Text   string          `yaml:"text"`
		Number int             `yaml:"number"`
		Values []string        `yaml:"values"`
		Nested map[string]bool `yaml:"nested"`
	}
	var c config
	if e := DecodeConfig([]byte("text: '${NO_INTERPOLATION}'\nnumber: 12\nvalues: [one, two]\nnested: {enabled: true}\n"), &c); e != nil || c.Text != "${NO_INTERPOLATION}" || c.Number != 12 {
		t.Fatal(c, e)
	}
	for _, s := range []string{"[]", "text: x\ntext: y", "1: value", "text: 12", "number: '12'", "values: [1]", "nested: {enabled: 'true'}", "unknown: field", "text: &v abc\nvalues: [*v]", "<<: {text: value}", "text: value\n---\ntext: value", strings.Repeat("x", MaxConfigBytes+1)} {
		if e := DecodeConfig([]byte(s), &config{}); e == nil {
			t.Fatalf("accepted %q", s[:min(len(s), 100)])
		}
	}
	if p, e := ResolvePath("/base", "../relative"); e != nil || p != "/relative" {
		t.Fatal(p, e)
	}
	if _, e := ResolvePath("relative", "file"); e == nil {
		t.Fatal("relative base accepted")
	}
	if p, e := ResolvePath("/base", "/absolute"); e != nil || p != "/absolute" {
		t.Fatal(p, e)
	}
	_ = os.Getenv("NO_INTERPOLATION")
}
func TestOptionsAndBackoff(t *testing.T) {
	o := StartOptions{Executable: "/fixture", Init: Initialization{Name: "test", BaseDir: "/"}}
	if e := o.defaults(); e != nil || o.MaxMessageBytes != 16<<20 || o.Concurrency != 4 || o.StartupTimeout != 30*time.Second || o.ShutdownTimeout != 5*time.Second {
		t.Fatal(o, e)
	}
	for _, mutate := range []func(*StartOptions){func(o *StartOptions) { o.CallTimeout = -1 }, func(o *StartOptions) { o.Env = map[string]string{"PLUGIN_CLIENT_CERT": "override"} }, func(o *StartOptions) { o.MaxMessageBytes = MaxMessageBytes + 1 }, func(o *StartOptions) { o.Restart.JitterFraction = 2 }, func(o *StartOptions) { o.Restart.Multiplier = .5 }, func(o *StartOptions) { o.Restart.MaxAttempts = -1 }} {
		v := o
		mutate(&v)
		if v.defaults() == nil {
			t.Fatal("invalid options accepted")
		}
	}
	r := RestartPolicy{InitialDelay: time.Second, MaxDelay: 4 * time.Second, Multiplier: 2, JitterFraction: .2}
	for attempt, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second} {
		if got := restartDelay(r, attempt, .5); got != want {
			t.Fatal(got, want)
		}
	}
	if got := restartDelay(r, 1000000, 1); got != r.MaxDelay {
		t.Fatal("overflow", got)
	}
}

type legacyConfig string

func (*legacyConfig) UnmarshalYAML(unmarshal func(any) error) error { return nil }
func TestLegacyCustomDecodingCannotBypassStrictness(t *testing.T) {
	var v struct {
		Value legacyConfig `yaml:"value"`
	}
	if e := DecodeConfig([]byte("value: value"), &v); e == nil {
		t.Fatal("legacy custom decoder bypassed strictness")
	}
}

func TestStrictFloatTypes(t *testing.T) {
	var v struct {
		Value float64 `yaml:"value"`
	}
	if e := DecodeConfig([]byte("value: 1"), &v); e == nil {
		t.Fatal("implicit integer to float coercion")
	}
	if e := DecodeConfig([]byte("value: 1.0"), &v); e != nil || v.Value != 1 {
		t.Fatal(v, e)
	}
}

// A key decoder must not execute even when its underlying kind is string.
type configTextKey string

var configKeyDecoderCalled bool

func (k *configTextKey) UnmarshalText(data []byte) error {
	configKeyDecoderCalled = true
	*k = configTextKey("custom:" + string(data))
	return nil
}

func TestStrictConfigurationMapKeys(t *testing.T) {
	configKeyDecoderCalled = false
	var v struct {
		Values map[configTextKey]string `yaml:"values"`
	}
	if e := DecodeConfig([]byte("values: {original: value}\n"), &v); !errors.Is(e, ErrInvalid) {
		t.Fatalf("custom key decoder accepted: %v", e)
	}
	if configKeyDecoderCalled || v.Values != nil {
		t.Fatal("decoder ran or destination mutated")
	}
	var legacy map[legacyConfig]string
	if e := DecodeConfig([]byte("original: value"), &legacy); !errors.Is(e, ErrInvalid) {
		t.Fatalf("legacy YAML key decoder accepted: %v", e)
	}
	type plainKey string
	var plain map[plainKey]string
	if e := DecodeConfig([]byte("original: value"), &plain); e != nil || plain["original"] != "value" {
		t.Fatal(plain, e)
	}
}
