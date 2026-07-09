package service

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// fakeDLQRepo is an in-memory domain.DLQRepository for the DLQ service tests. It
// keeps dead letters by id and serves List newest-first (failed_at desc), which
// is enough to exercise the redeliver orchestration without a Mongo.
type fakeDLQRepo struct {
	mu   sync.Mutex
	byID map[string]domain.DeadLetter
}

func newFakeDLQRepo() *fakeDLQRepo {
	return &fakeDLQRepo{byID: map[string]domain.DeadLetter{}}
}

func (r *fakeDLQRepo) Save(_ context.Context, dl domain.DeadLetter) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[dl.ID] = dl
	return nil
}

func (r *fakeDLQRepo) List(_ context.Context, ruleID string, limit int, _ *domain.DLQCursor) ([]domain.DeadLetter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []domain.DeadLetter{}
	for _, dl := range r.byID {
		if dl.RuleID == ruleID {
			out = append(out, dl)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FailedAt.After(out[j].FailedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeDLQRepo) Get(_ context.Context, id string) (*domain.DeadLetter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	dl, ok := r.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &dl, nil
}

func (r *fakeDLQRepo) Delete(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[id]; !ok {
		return domain.ErrNotFound
	}
	delete(r.byID, id)
	return nil
}

func (r *fakeDLQRepo) Count(_ context.Context, ruleID string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, dl := range r.byID {
		if ruleID == "" || dl.RuleID == ruleID {
			n++
		}
	}
	return n, nil
}

func (r *fakeDLQRepo) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byID)
}

// stubRequeuer records requeued delivery ids and can be made to fail.
type stubRequeuer struct {
	mu       sync.Mutex
	requeued []string
	failWith error
}

func (s *stubRequeuer) Requeue(_ context.Context, dl domain.DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return s.failWith
	}
	s.requeued = append(s.requeued, dl.DeliveryID)
	return nil
}

func (s *stubRequeuer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requeued)
}

func seedDL(repo *fakeDLQRepo, id, ruleID, deliveryID string, at time.Time) {
	_ = repo.Save(context.Background(), domain.DeadLetter{
		ID: id, RuleID: ruleID, DeliveryID: deliveryID, FailedAt: at,
	})
}

func TestDLQRedeliverIDs(t *testing.T) {
	repo := newFakeDLQRepo()
	base := time.Unix(1000, 0).UTC()
	seedDL(repo, "dlq_1", "rule_a", "dlv_1", base)
	seedDL(repo, "dlq_2", "rule_a", "dlv_2", base.Add(time.Second))
	req := &stubRequeuer{}
	svc := NewDLQService(repo, req, nil)

	n, err := svc.Redeliver(authedCtx(), "rule_a", []string{"dlq_1", "dlq_2"})
	if err != nil {
		t.Fatalf("redeliver: %v", err)
	}
	if n != 2 {
		t.Fatalf("requeued = %d, want 2", n)
	}
	if req.count() != 2 {
		t.Fatalf("requeuer saw %d, want 2", req.count())
	}
	if repo.len() != 0 {
		t.Fatalf("entries left after redeliver = %d, want 0", repo.len())
	}
}

func TestDLQRedeliverUnknownIDIsNotFound(t *testing.T) {
	repo := newFakeDLQRepo()
	svc := NewDLQService(repo, &stubRequeuer{}, nil)

	_, err := svc.Redeliver(authedCtx(), "rule_a", []string{"dlq_missing"})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestDLQRedeliverWrongRuleIsNotFound(t *testing.T) {
	repo := newFakeDLQRepo()
	seedDL(repo, "dlq_1", "rule_b", "dlv_1", time.Now())
	svc := NewDLQService(repo, &stubRequeuer{}, nil)

	// The id exists but under a different rule: the rule-scoped path must not reach
	// it.
	_, err := svc.Redeliver(authedCtx(), "rule_a", []string{"dlq_1"})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if repo.len() != 1 {
		t.Fatalf("entry was touched: %d left", repo.len())
	}
}

func TestDLQRedeliverAllDrainsRule(t *testing.T) {
	repo := newFakeDLQRepo()
	base := time.Unix(1000, 0).UTC()
	seedDL(repo, "dlq_1", "rule_a", "dlv_1", base)
	seedDL(repo, "dlq_2", "rule_a", "dlv_2", base.Add(time.Second))
	seedDL(repo, "dlq_3", "rule_other", "dlv_3", base) // a different rule, untouched
	req := &stubRequeuer{}
	svc := NewDLQService(repo, req, nil)

	n, err := svc.Redeliver(authedCtx(), "rule_a", nil)
	if err != nil {
		t.Fatalf("redeliver all: %v", err)
	}
	if n != 2 {
		t.Fatalf("requeued = %d, want 2", n)
	}
	left, _ := repo.List(authedCtx(), "rule_other", 0, nil)
	if len(left) != 1 {
		t.Fatalf("other rule's DLQ was touched: %d left", len(left))
	}
}

func TestDLQRedeliverKeepsEntryWhenRequeueFails(t *testing.T) {
	repo := newFakeDLQRepo()
	seedDL(repo, "dlq_1", "rule_a", "dlv_1", time.Now())
	req := &stubRequeuer{failWith: errors.New("queue down")}
	svc := NewDLQService(repo, req, nil)

	_, err := svc.Redeliver(authedCtx(), "rule_a", []string{"dlq_1"})
	if err == nil {
		t.Fatal("expected an error when requeue fails")
	}
	if repo.len() != 1 {
		t.Fatalf("entry was deleted despite requeue failure: %d left", repo.len())
	}
}

func TestDLQRedeliverEmptyIsZero(t *testing.T) {
	repo := newFakeDLQRepo()
	svc := NewDLQService(repo, &stubRequeuer{}, nil)

	n, err := svc.Redeliver(authedCtx(), "rule_a", nil)
	if err != nil {
		t.Fatalf("redeliver: %v", err)
	}
	if n != 0 {
		t.Fatalf("requeued = %d, want 0", n)
	}
}
