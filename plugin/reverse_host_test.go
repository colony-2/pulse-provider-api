package plugin

import (
	"context"
	"errors"
	"testing"

	api "github.com/colony-2/pulse-provider-api"
)

// This fixture models only the root contract's interpretation of reads. It does
// not install scheduling or fallback policy in the SDK or in production Pulse.
func hostCatalogIncompatible(ctx context.Context, p api.Provider) (bool, error) {
	c, e := p.GetCapabilities(ctx)
	if e != nil {
		return false, e
	}
	return c.BasesComplete && len(c.Bases) == 0, nil
}
func hostInventoryEmpty(ctx context.Context, p api.Provider) (bool, error) {
	v, e := p.List(ctx, api.ListRequest{})
	if e != nil {
		return false, e
	}
	return len(v.Items) == 0, nil
}
func TestReverseHostIgnoresFailedReadData(t *testing.T) {
	tr := testTrust(t)
	c := testCollector(t, collectorOptions(tr))
	var readError error
	p := &reverseProvider{caps: func(context.Context) (api.Capabilities, error) {
		return api.Capabilities{BasesComplete: true}, readError
	}, list: func(context.Context, api.ListRequest) (api.ListResponse, error) { return api.ListResponse{}, readError }, submit: func(context.Context, []api.Launch) ([]api.Submission, error) { return nil, errors.New("uncertain") }}
	_, done := launchRun(t, factoryFor(p), runOptions(t, c, tr, "alpha"))
	handle := nextSnapshot(t, c, 1).Providers[0].Provider
	// Identical complete/empty data establish incompatibility/emptiness only when
	// the read succeeds. Method errors must block those conclusions.
	incompatible, e := hostCatalogIncompatible(context.Background(), handle)
	if e != nil || !incompatible {
		t.Fatal(incompatible, e)
	}
	empty, e := hostInventoryEmpty(context.Background(), handle)
	if e != nil || !empty {
		t.Fatal(empty, e)
	}
	ss, e := handle.Submit(context.Background(), launches("uncertain"))
	if e == nil || ss[0].Status != api.Unknown {
		t.Fatal(ss, e)
	}
	readError = errors.New("failed read")
	incompatible, e = hostCatalogIncompatible(context.Background(), handle)
	if e == nil || incompatible {
		t.Fatal(incompatible, e)
	}
	empty, e = hostInventoryEmpty(context.Background(), handle)
	if e == nil || empty {
		t.Fatal(empty, e)
	}
	if ss[0].Status != api.Unknown || p.calls.Load() != 1 {
		t.Fatal("failed empty read resolved/replayed uncertain launch")
	}
	_ = c.Close()
	receiveRun(t, done)
}
