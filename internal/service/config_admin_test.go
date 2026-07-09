package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
)

// fakeInvalidator records which collections had their cache dropped.
type fakeInvalidator struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeInvalidator) Invalidate(_ context.Context, collection string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, collection)
}

// fakeAudit captures recorded audit events; err lets a test force a failure.
type fakeAudit struct {
	mu     sync.Mutex
	events []domain.AuditEvent
	err    error
}

func (f *fakeAudit) Record(_ context.Context, e domain.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, e)
	return nil
}

func TestUpdateConfigPersistsProvisionsInvalidatesAudits(t *testing.T) {
	repo := newFakeRepo()
	inv := &fakeInvalidator{}
	audit := &fakeAudit{}
	svc := NewCollectionConfigService(repo, inv, audit, nil)

	in := &domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: domain.ReliabilityStrict}
	got, err := svc.Update(authedCtx(), in)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Effective config: explicit field kept, the rest defaulted.
	if got.ReliabilityMode != domain.ReliabilityStrict {
		t.Errorf("reliability_mode = %q, want strict", got.ReliabilityMode)
	}
	if got.SnapshotInterval != domain.DefaultSnapshotInterval || got.FirstEventOp != domain.FirstEventCreate {
		t.Errorf("defaults not applied: %+v", got)
	}
	// Physical collections provisioned; the stored record keeps only what was set
	// (raw), so GET can later mark the rest as defaults.
	if repo.saveCalls != 1 || repo.ensureCalls != 1 {
		t.Fatalf("save=%d ensure=%d, want 1/1", repo.saveCalls, repo.ensureCalls)
	}
	if stored := repo.configs["crm.deals"]; stored.SnapshotInterval != 0 || stored.ReliabilityMode != domain.ReliabilityStrict {
		t.Errorf("stored config should be raw (set fields only): %+v", stored)
	}
	// Cache invalidated so the new mode is seen immediately (S1-04).
	if len(inv.calls) != 1 || inv.calls[0] != "crm.deals" {
		t.Errorf("invalidate calls = %v, want [crm.deals]", inv.calls)
	}
	// Audited (NFR-5.6): who/what/which.
	if len(audit.events) != 1 {
		t.Fatalf("audit events = %d, want 1", len(audit.events))
	}
	ev := audit.events[0]
	if ev.Action != "collection.config.updated" || ev.Collection != "crm.deals" || ev.Actor != "acme" {
		t.Errorf("audit event = %+v", ev)
	}
	if ev.ID == "" || ev.TS.IsZero() {
		t.Errorf("audit event missing id/ts: %+v", ev)
	}
	if ev.Details["reliability_mode"] != "strict" {
		t.Errorf("audit details = %+v", ev.Details)
	}
}

func TestUpdateConfigRejectsInvalid(t *testing.T) {
	repo := newFakeRepo()
	svc := NewCollectionConfigService(repo, &fakeInvalidator{}, &fakeAudit{}, nil)

	_, err := svc.Update(authedCtx(), &domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: "loud"})
	var ce *domain.ConfigError
	if !errors.As(err, &ce) || ce.Field != "reliability_mode" {
		t.Fatalf("err = %v, want ConfigError on reliability_mode", err)
	}
	// Nothing must be written when validation fails.
	if repo.saveCalls != 0 || repo.ensureCalls != 0 {
		t.Fatalf("invalid config must not persist: save=%d ensure=%d", repo.saveCalls, repo.ensureCalls)
	}
}

// A failed audit write must not fail an already-applied config change.
func TestUpdateConfigAuditFailureIsSwallowed(t *testing.T) {
	repo := newFakeRepo()
	audit := &fakeAudit{err: errors.New("ev__system down")}
	svc := NewCollectionConfigService(repo, &fakeInvalidator{}, audit, nil)

	if _, err := svc.Update(authedCtx(), &domain.CollectionConfig{Name: "crm.deals"}); err != nil {
		t.Fatalf("Update must succeed despite audit failure: %v", err)
	}
	if repo.saveCalls != 1 {
		t.Fatalf("config should still be saved, saveCalls=%d", repo.saveCalls)
	}
}

// A nil audit store is allowed (e.g. before wiring); Update still works.
func TestUpdateConfigNilAudit(t *testing.T) {
	repo := newFakeRepo()
	svc := NewCollectionConfigService(repo, &fakeInvalidator{}, nil, nil)
	if _, err := svc.Update(authedCtx(), &domain.CollectionConfig{Name: "crm.deals"}); err != nil {
		t.Fatalf("Update with nil audit: %v", err)
	}
}

func TestGetStoredPassesThroughNotFound(t *testing.T) {
	repo := newFakeRepo()
	svc := NewCollectionConfigService(repo, &fakeInvalidator{}, nil, nil)

	if _, err := svc.GetStored(authedCtx(), "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}

	repo.configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: domain.ReliabilityFast}
	got, err := svc.GetStored(authedCtx(), "crm.deals")
	if err != nil {
		t.Fatalf("GetStored: %v", err)
	}
	// Raw stored config — defaults NOT applied (so transport can mark them).
	if got.ReliabilityMode != domain.ReliabilityFast || got.SnapshotInterval != 0 {
		t.Fatalf("GetStored should return raw config, got %+v", got)
	}
}
