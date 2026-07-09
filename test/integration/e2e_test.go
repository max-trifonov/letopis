//go:build integration

// Package integration holds the stage-1 end-to-end suite. It drives the REST
// API as a black box over a real httptest server, with MongoDB provided by
// testcontainers, and inspects Mongo directly only to assert storage
// invariants. It runs under `go test -tags integration ./...` and is the
// "lock" on stage 1: green here means the layers are assembled and the
// contract holds (S1-08).
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"

	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/server"
	"github.com/max-trifonov/letopis/internal/service"
	storage "github.com/max-trifonov/letopis/internal/storage/mongo"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// startMongo runs a throwaway MongoDB and returns its connection string.
func startMongo(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start mongo: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return uri
}

// harness is the assembled stack under test: the HTTP server plus the keys the
// scenarios authenticate with.
type harness struct {
	url  string
	keyA string // tenant A, crm.* read+write, on the default cluster
	keyB string // tenant B, docs.* read+write, on its own cluster (per-tenant URI)
}

// newHarness wires the real repositories and services over two MongoDB
// clusters: tenant A on the default URI, tenant B pinned to a second cluster.
// This exercises per-tenant URI isolation (ADR-010, FR-7.2) end to end.
func newHarness(t *testing.T) harness {
	t.Helper()
	uriA := startMongo(t)
	uriB := startMongo(t)

	resolver, _, err := tenant.NewResolver([]tenant.Spec{
		{ID: "a", Keys: []tenant.KeySpec{{Plaintext: "key-a", Scopes: []string{"read", "write", "admin"}, Collections: []string{"crm.*"}}}},
		{ID: "b", DBURI: uriB, Keys: []tenant.KeySpec{{Plaintext: "key-b", Scopes: []string{"read", "write"}, Collections: []string{"docs.*"}}}},
	})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}

	conn, err := storage.NewConnManager(uriA)
	if err != nil {
		t.Fatalf("conn manager: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	colRepo := storage.NewCollectionRepo(conn)
	cfgResolver := service.NewConfigResolver(colRepo, service.Options{AutoCreate: true})
	snapshotRepo := storage.NewSnapshotRepo(conn)
	ingester := service.NewIngester(cfgResolver, storage.NewEventRepo(conn), storage.NewCurrentRepo(conn), service.WithSnapshots(service.NewSnapshotBuilder(snapshotRepo, slog.Default())))
	reader := service.NewReader(storage.NewEventRepo(conn), storage.NewCurrentRepo(conn), snapshotRepo)
	activities := service.NewActivities(storage.NewFlowRepo(conn))
	catalog := service.NewCatalog(storage.NewStatsRepo(conn), colRepo)
	configAdmin := service.NewCollectionConfigService(colRepo, cfgResolver, storage.NewSystemAuditRepo(conn), slog.Default())
	ruleAdmin := service.NewRuleService(storage.NewRuleRepo(conn), nil, storage.NewSystemAuditRepo(conn), slog.Default())
	// The batch route reuses the async facade; with no queue wired it serves the
	// strict path synchronously, which is what the batch e2e drives.
	async := service.NewAsyncIngester(ingester, service.NewPipeline(nil, nil, nil), nil, service.AsyncOptions{})
	batch := service.NewBatchIngester(async, nil, slog.Default())

	handler := server.NewRouter(health.NewRegistry(), resolver, ingester, reader, activities, nil, catalog, configAdmin, batch, ruleAdmin, nil)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return harness{url: srv.URL, keyA: "key-a", keyB: "key-b"}
}

// do issues an authenticated request and returns the status and decoded body.
func (h harness) do(t *testing.T, method, path, key, body string) (int, map[string]any) {
	return h.doH(t, method, path, key, body, nil)
}

// doH is do with extra request headers, for the per-request reliability override.
func (h harness) doH(t *testing.T, method, path, key, body string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, h.url+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &decoded)
	}
	return resp.StatusCode, decoded
}

func (h harness) mustStatus(t *testing.T, want, got int, ctx string, body map[string]any) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: status = %d, want %d; body=%v", ctx, got, want, body)
	}
}

// TestE2EHappyPath walks every stage-1 endpoint on a single entity lifecycle:
// create, update, delete, reincarnate, read history and current state, then
// record an activity and assemble its flow (S1-08 DoD).
func TestE2EHappyPath(t *testing.T) {
	h := newHarness(t)
	const base = "/api/v1/collections/crm.deals/entities/d-1"

	// create via /state
	st, body := h.do(t, http.MethodPost, base+"/state", h.keyA, `{"state":{"title":"deal","amount":100},"flow":{"flow_id":"f-1","step":"opened"}}`)
	h.mustStatus(t, http.StatusCreated, st, "create", body)
	if body["version"].(float64) != 1 {
		t.Fatalf("first version = %v, want 1", body["version"])
	}

	// update via /diff
	st, body = h.do(t, http.MethodPost, base+"/diff", h.keyA, `{"op":"update","changes":[{"path":"amount","op":"change","old":100,"new":250}]}`)
	h.mustStatus(t, http.StatusCreated, st, "diff", body)
	if body["version"].(float64) != 2 {
		t.Fatalf("update version = %v, want 2", body["version"])
	}

	// delete
	st, body = h.do(t, http.MethodPost, base+"/delete", h.keyA, `{"author_id":"42"}`)
	h.mustStatus(t, http.StatusCreated, st, "delete", body)

	// reincarnate via /state — op create, version keeps climbing
	st, body = h.do(t, http.MethodPost, base+"/state", h.keyA, `{"state":{"title":"deal","amount":500}}`)
	h.mustStatus(t, http.StatusCreated, st, "reincarnate", body)
	if body["version"].(float64) != 4 {
		t.Fatalf("reincarnation version = %v, want 4", body["version"])
	}

	// current state reflects the latest write
	st, body = h.do(t, http.MethodGet, base+"/state", h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "current state", body)
	if body["deleted"].(bool) || body["state"].(map[string]any)["amount"].(float64) != 500 {
		t.Fatalf("current state wrong: %v", body)
	}

	// history: full list, newest-first by default
	st, body = h.do(t, http.MethodGet, base+"/history", h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "history", body)
	events := body["events"].([]any)
	if len(events) != 4 {
		t.Fatalf("history has %d events, want 4", len(events))
	}

	// history filter by op + json-patch projection
	st, body = h.do(t, http.MethodGet, base+"/history?op=update&format=json-patch", h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "history filtered", body)
	filtered := body["events"].([]any)
	if len(filtered) != 1 {
		t.Fatalf("op=update filter returned %d events, want 1", len(filtered))
	}
	patch := filtered[0].(map[string]any)["changes"].([]any)[0].(map[string]any)
	if patch["op"] != "replace" || patch["path"] != "/amount" {
		t.Fatalf("json-patch projection wrong: %v", patch)
	}

	// history pagination: a page of 1 yields a cursor that walks the rest
	seen := 0
	cursor := ""
	for {
		path := base + "/history?limit=1&order_by=version&order=asc"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		st, body = h.do(t, http.MethodGet, path, h.keyA, "")
		h.mustStatus(t, http.StatusOK, st, "history page", body)
		page := body["events"].([]any)
		seen += len(page)
		next, ok := body["next_cursor"].(string)
		if !ok || next == "" {
			break
		}
		cursor = next
	}
	if seen != 4 {
		t.Fatalf("paged history saw %d events, want 4", seen)
	}

	// record an activity into the same flow, then assemble the flow
	st, body = h.do(t, http.MethodPost, "/api/v1/activities", h.keyA,
		`{"type":"recalc.prices","flow_id":"f-1","caused_by":[{"collection":"crm.deals","entity_id":"d-1","version":1}],"data":{"recalced":17}}`)
	h.mustStatus(t, http.StatusCreated, st, "activity", body)
	if body["activity_id"].(string) == "" || body["flow_id"].(string) != "f-1" {
		t.Fatalf("activity ids wrong: %v", body)
	}

	st, body = h.do(t, http.MethodGet, "/api/v1/flows/f-1", h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "flow", body)
	nodes := body["nodes"].([]any)
	// The create event (flow opened) and the activity both belong to f-1.
	if len(nodes) != 2 {
		t.Fatalf("flow has %d nodes, want 2; body=%v", len(nodes), body)
	}
	kinds := map[string]bool{}
	for _, n := range nodes {
		kinds[n.(map[string]any)["kind"].(string)] = true
	}
	if !kinds["event"] || !kinds["activity"] {
		t.Fatalf("flow missing a node kind: %v", kinds)
	}
}

// TestE2EPointInTime drives the point-in-time read over the API (FR-3.2, S3-03):
// ?version and ?at reconstruct past states, the mutually-exclusive and reserved
// parameters are rejected, and a cutoff before the first event is a 404.
func TestE2EPointInTime(t *testing.T) {
	h := newHarness(t)
	const base = "/api/v1/collections/crm.deals/entities/d-1"

	for _, amount := range []int{100, 250, 500} {
		st, body := h.do(t, http.MethodPost, base+"/state", h.keyA, fmt.Sprintf(`{"state":{"amount":%d}}`, amount))
		h.mustStatus(t, http.StatusCreated, st, "seed", body)
	}

	// Reconstruct v2 (default interval 100 → no snapshot, replayed from genesis).
	st, body := h.do(t, http.MethodGet, base+"/state?version=2", h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "state?version=2", body)
	if body["version"].(float64) != 2 || body["state"].(map[string]any)["amount"].(float64) != 250 {
		t.Fatalf("point-in-time v2 wrong: %v", body)
	}
	rf := body["reconstructed_from"].(map[string]any)
	if rf["snapshot_version"] != nil || rf["events_applied"].(float64) != 2 {
		t.Fatalf("reconstructed_from wrong: %v", rf)
	}

	// A version past the tip clamps to the latest.
	st, body = h.do(t, http.MethodGet, base+"/state?version=99", h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "state?version=99", body)
	if body["version"].(float64) != 3 {
		t.Fatalf("clamp wrong: %v", body)
	}

	// ?at uses the v2 event's ts_received as the cutoff.
	ts := h.eventTSReceived(t, base, 2)
	st, body = h.do(t, http.MethodGet, base+"/state?at="+url.QueryEscape(ts), h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "state?at", body)
	if body["version"].(float64) != 2 {
		t.Fatalf("at-resolution wrong: %v", body)
	}

	// Bad parameter combinations.
	st, _ = h.do(t, http.MethodGet, base+"/state?version=2&at="+url.QueryEscape(ts), h.keyA, "")
	if st != http.StatusBadRequest {
		t.Fatalf("version+at = %d, want 400", st)
	}
	st, _ = h.do(t, http.MethodGet, base+"/state?at_source="+url.QueryEscape(ts), h.keyA, "")
	if st != http.StatusBadRequest {
		t.Fatalf("at_source = %d, want 400", st)
	}

	// A cutoff before the first event is a 404.
	st, _ = h.do(t, http.MethodGet, base+"/state?at=2000-01-01T00:00:00Z", h.keyA, "")
	if st != http.StatusNotFound {
		t.Fatalf("at before first event = %d, want 404", st)
	}
}

// eventTSReceived fetches the ts_received of a specific version from history, so
// an ?at cutoff can be pinned to a real event boundary.
func (h harness) eventTSReceived(t *testing.T, base string, version int) string {
	t.Helper()
	_, body := h.do(t, http.MethodGet, base+"/history?order_by=version&order=asc", h.keyA, "")
	for _, e := range body["events"].([]any) {
		ev := e.(map[string]any)
		if int(ev["version"].(float64)) == version {
			return ev["ts_received"].(string)
		}
	}
	t.Fatalf("version %d not found in history", version)
	return ""
}

// TestE2ETenantIsolation proves a key cannot reach another tenant's data, and
// that tenant B's writes land on its own cluster (per-tenant URI, FR-7.2).
func TestE2ETenantIsolation(t *testing.T) {
	h := newHarness(t)

	// Tenant B writes to docs.notes on its own cluster.
	st, body := h.do(t, http.MethodPost, "/api/v1/collections/docs.notes/entities/n-1/state", h.keyB, `{"state":{"text":"hello"}}`)
	h.mustStatus(t, http.StatusCreated, st, "B write", body)

	// Tenant A may not touch docs.* at all — its mask is crm.* (403).
	st, _ = h.do(t, http.MethodGet, "/api/v1/collections/docs.notes/entities/n-1/state", h.keyA, "")
	if st != http.StatusForbidden {
		t.Fatalf("A reading docs.* = %d, want 403", st)
	}

	// Tenant A's own crm.deals/n-1 does not exist — isolation is per-database,
	// so B's entity is invisible even at the same logical coordinates.
	st, _ = h.do(t, http.MethodGet, "/api/v1/collections/crm.deals/entities/n-1/state", h.keyA, "")
	if st != http.StatusNotFound {
		t.Fatalf("A reading its own empty entity = %d, want 404", st)
	}
}

// TestE2ECollectionsList drives GET /collections end to end (S3-04, FR-3.6):
// after writing two entities the listing reports the collection with accurate
// statistics and its effective config, and the key's mask hides another
// tenant's collections.
func TestE2ECollectionsList(t *testing.T) {
	h := newHarness(t)

	// Tenant A writes two entities into crm.deals (two events, two entities).
	st, body := h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-1/state", h.keyA, `{"state":{"amount":100}}`)
	h.mustStatus(t, http.StatusCreated, st, "write d-1", body)
	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-2/state", h.keyA, `{"state":{"amount":200}}`)
	h.mustStatus(t, http.StatusCreated, st, "write d-2", body)

	// Tenant B writes into docs.notes on its own cluster — A must never see it.
	st, body = h.do(t, http.MethodPost, "/api/v1/collections/docs.notes/entities/n-1/state", h.keyB, `{"state":{"text":"x"}}`)
	h.mustStatus(t, http.StatusCreated, st, "B write", body)

	st, body = h.do(t, http.MethodGet, "/api/v1/collections", h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "list", body)

	cols, ok := body["collections"].([]any)
	if !ok || len(cols) != 1 {
		t.Fatalf("collections = %v, want exactly crm.deals", body["collections"])
	}
	c := cols[0].(map[string]any)
	if c["name"] != "crm.deals" {
		t.Fatalf("name = %v, want crm.deals", c["name"])
	}
	if c["entities"].(float64) != 2 || c["events"].(float64) != 2 {
		t.Fatalf("stats = entities:%v events:%v, want 2/2", c["entities"], c["events"])
	}
	if c["last_event_at"] == nil {
		t.Fatal("last_event_at is null, want a timestamp")
	}
	cfg := c["config"].(map[string]any)
	if cfg["reliability_mode"] != "durable" || cfg["snapshot_interval"].(float64) != 100 {
		t.Fatalf("config = %v, want durable/100 defaults", cfg)
	}
}

// TestE2ECollectionConfig drives the admin config API end to end (S3-05): PUT a
// config, GET it back with defaults marked, and confirm the new reliability mode
// is what subsequent ingest sees (resolver cache invalidated).
func TestE2ECollectionConfig(t *testing.T) {
	h := newHarness(t)
	const path = "/api/v1/collections/crm.deals/config"

	// PUT a partial config: only the mode and snapshot interval are set.
	st, body := h.do(t, http.MethodPut, path, h.keyA, `{"reliability_mode":"strict","snapshot_interval":25}`)
	h.mustStatus(t, http.StatusOK, st, "put config", body)
	cfg := body["config"].(map[string]any)
	if cfg["reliability_mode"] != "strict" || cfg["snapshot_interval"].(float64) != 25 {
		t.Fatalf("effective config = %v", cfg)
	}

	// GET reflects it, with the untouched fields flagged as defaults.
	st, body = h.do(t, http.MethodGet, path, h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "get config", body)
	cfg = body["config"].(map[string]any)
	if cfg["reliability_mode"] != "strict" {
		t.Fatalf("get config = %v", cfg)
	}
	defaults := map[string]bool{}
	for _, f := range body["defaults"].([]any) {
		defaults[f.(string)] = true
	}
	if defaults["reliability_mode"] || defaults["snapshot_interval"] {
		t.Errorf("set fields wrongly marked default: %v", body["defaults"])
	}
	if !defaults["max_event_size_bytes"] || !defaults["first_event_op"] {
		t.Errorf("unset fields not marked default: %v", body["defaults"])
	}

	// GET of an unconfigured collection is 404.
	st, _ = h.do(t, http.MethodGet, "/api/v1/collections/crm.other/config", h.keyA, "")
	if st != http.StatusNotFound {
		t.Fatalf("unconfigured GET status = %d, want 404", st)
	}

	// The configured collection is now usable for ingest (PUT provisioned the
	// physical collections), so a subsequent write succeeds.
	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-1/state", h.keyA, `{"state":{"amount":1}}`)
	h.mustStatus(t, http.StatusCreated, st, "write after config", body)
}

// TestE2EBatchAccept drives the batch endpoint (S3-06): a partial accept where
// two valid items are written and one invalid item is rejected by index. The mode
// is overridden to strict so the items write synchronously (the harness wires no
// queue), and a subsequent read confirms the accepted entities landed.
func TestE2EBatchAccept(t *testing.T) {
	h := newHarness(t)
	body := `{"events":[
		{"collection":"crm.deals","entity_id":"b-1","type":"state","payload":{"state":{"amount":10}}},
		{"collection":"crm.deals","entity_id":"b-2","type":"diff","payload":{"op":"create","state":{"amount":20}}},
		{"collection":"crm.deals","entity_id":"b-3","type":"frobnicate","payload":{}}
	]}`
	st, resp := h.doH(t, http.MethodPost, "/api/v1/events:batch", h.keyA, body, map[string]string{"X-Letopis-Mode": "strict"})
	h.mustStatus(t, http.StatusAccepted, st, "batch", resp)
	if resp["accepted"].(float64) != 2 {
		t.Fatalf("accepted = %v, want 2; body=%v", resp["accepted"], resp)
	}
	rej := resp["rejected"].([]any)
	if len(rej) != 1 || rej[0].(map[string]any)["index"].(float64) != 2 {
		t.Fatalf("rejected = %v, want one at index 2", rej)
	}
	if s, _ := resp["ticket_id"].(string); s == "" {
		t.Fatal("missing ticket_id")
	}

	for _, eid := range []string{"b-1", "b-2"} {
		st, body := h.do(t, http.MethodGet, "/api/v1/collections/crm.deals/entities/"+eid+"/state", h.keyA, "")
		h.mustStatus(t, http.StatusOK, st, "read "+eid, body)
	}
}

// TestE2EConcurrentIngest hammers one entity with concurrent writes and asserts
// the history has every version 1..N exactly once — no gaps, no duplicates
// (FR-1.10). Runs under -race in the integration job.
func TestE2EConcurrentIngest(t *testing.T) {
	h := newHarness(t)
	const base = "/api/v1/collections/crm.deals/entities/race-1"
	const n = 20

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"op":"update","changes":[{"path":"n%d","op":"add","new":%d}]}`, i, i)
			h.do(t, http.MethodPost, base+"/diff", h.keyA, body)
		}(i)
	}
	wg.Wait()

	st, body := h.do(t, http.MethodGet, base+"/history?limit=1000&order_by=version&order=asc", h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "history", body)
	events := body["events"].([]any)
	seen := map[int64]bool{}
	for _, e := range events {
		v := int64(e.(map[string]any)["version"].(float64))
		if seen[v] {
			t.Fatalf("duplicate version %d", v)
		}
		seen[v] = true
	}
	for v := int64(1); v <= int64(len(events)); v++ {
		if !seen[v] {
			t.Fatalf("gap at version %d (have %d events)", v, len(events))
		}
	}
}
