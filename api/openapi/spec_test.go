// Package openapi guards the hand-written API spec: the test parses and builds
// the OpenAPI 3.1 model, so a malformed contract fails CI like any other test
// (NFR-8.1). It is plain `go test` — no Docker, no network — so it runs in the
// default suite, not behind the integration tag.
package openapi

import (
	"os"
	"testing"

	"github.com/pb33f/libopenapi"
)

func TestSpecBuilds(t *testing.T) {
	raw, err := os.ReadFile("letopis.v1.yaml")
	if err != nil {
		t.Fatalf("read spec: %v", err)
	}

	doc, err := libopenapi.NewDocument(raw)
	if err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	if got := doc.GetSpecInfo().VersionNumeric; got < 3.1 {
		t.Fatalf("spec version = %v, want >= 3.1", got)
	}

	model, err := doc.BuildV3Model()
	if err != nil {
		t.Fatalf("build model: %v", err)
	}

	// Every stage-1 endpoint must be present, so the contract cannot silently
	// drift from the implemented surface (S1-08 DoD).
	wantPaths := []string{
		"/collections/{collection}/entities/{entityId}/state",
		"/collections/{collection}/entities/{entityId}/diff",
		"/collections/{collection}/entities/{entityId}/delete",
		"/collections/{collection}/entities/{entityId}/history",
		"/activities",
		"/flows/{flowId}",
		"/tickets/{ticketId}",
		"/collections",
		"/collections/{collection}/config",
		"/collections/{collection}/rules",
		"/collections/{collection}/rules/{ruleId}",
		"/collections/{collection}/rules/{ruleId}/dlq",
		"/collections/{collection}/rules/{ruleId}/dlq:redeliver",
		"/events:batch",
	}
	paths := model.Model.Paths.PathItems
	for _, p := range wantPaths {
		if _, ok := paths.Get(p); !ok {
			t.Errorf("spec missing path %q", p)
		}
	}

	// The point-in-time read (S3-03) is the stage-3 contract surface most prone to
	// silent drift: assert GET /state declares the ?version and ?at query
	// parameters and that ReconstructedState exposes reconstructed_from. The GET
	// lives under the state-read key because OpenAPI keys paths by URL and the GET
	// shares the /state URL with the ingest POST.
	statePath, ok := paths.Get("/collections/{collection}/entities/{entityId}/state-read")
	if !ok || statePath.Get == nil {
		t.Fatal("spec missing GET .../state (state-read)")
	}
	params := map[string]bool{}
	for _, p := range statePath.Get.Parameters {
		params[p.Name] = true
	}
	for _, want := range []string{"version", "at"} {
		if !params[want] {
			t.Errorf("GET /state missing point-in-time query param %q", want)
		}
	}

	schemas := model.Model.Components.Schemas
	recon, ok := schemas.Get("ReconstructedState")
	if !ok {
		t.Fatal("spec missing schema ReconstructedState")
	}
	if _, ok := recon.Schema().Properties.Get("reconstructed_from"); !ok {
		t.Error("ReconstructedState missing reconstructed_from property")
	}

	// The hash-chain link (S5-02) is exposed on history events and is a versioned
	// verification contract, so assert the schema and that an event references it.
	intg, ok := schemas.Get("Integrity")
	if !ok {
		t.Fatal("spec missing schema Integrity")
	}
	if _, ok := intg.Schema().Properties.Get("hash"); !ok {
		t.Error("Integrity missing hash property")
	}
	hev, ok := schemas.Get("HistoryEvent")
	if !ok {
		t.Fatal("spec missing schema HistoryEvent")
	}
	if _, ok := hev.Schema().Properties.Get("integrity"); !ok {
		t.Error("HistoryEvent missing integrity property")
	}

	// The rules contract (S4-03) is recursive (Condition references itself) and
	// hand-written, so assert its key schemas resolve and the condition's operator
	// keys are declared — the surface most prone to silent drift from the engine.
	for _, name := range []string{"Rule", "RuleRequest", "Condition", "Match", "Action", "DeadLetter"} {
		if _, ok := schemas.Get(name); !ok {
			t.Errorf("spec missing schema %q", name)
		}
	}
	cond, ok := schemas.Get("Condition")
	if !ok {
		t.Fatal("spec missing schema Condition")
	}
	for _, op := range []string{"all", "any", "not", "field", "eq", "regex", "exists", "match"} {
		if _, ok := cond.Schema().Properties.Get(op); !ok {
			t.Errorf("Condition schema missing property %q", op)
		}
	}
}
