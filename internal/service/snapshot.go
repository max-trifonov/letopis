package service

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// SnapshotBuilder materializes sn_* snapshots on the version interval, post-store
// best-effort. A failed snapshot only means later reads replay further; it must
// never fail the write it follows.
type SnapshotBuilder struct {
	repo domain.SnapshotRepository
	now  func() time.Time
	log  *slog.Logger
}

func NewSnapshotBuilder(repo domain.SnapshotRepository, log *slog.Logger) *SnapshotBuilder {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &SnapshotBuilder{repo: repo, now: time.Now, log: log.With("component", "snapshot-builder")}
}

// Build persists a snapshot at an interval boundary; off a boundary it is a no-op.
// A nil builder is a no-op. Save upserts by {entity,version} so reclaim
// reprocessing re-snaps the same slot rather than duplicating it.
func (b *SnapshotBuilder) Build(ctx context.Context, collection, entityID string, version int64, ts time.Time, state map[string]any, deleted bool, interval int) {
	if b == nil || !domain.ShouldSnapshot(version, interval) {
		return
	}
	snap := &domain.Snapshot{EntityID: entityID, Version: version, TS: ts, Deleted: deleted, State: state}
	if err := b.repo.Save(ctx, collection, snap); err != nil {
		b.log.Warn("snapshot build failed", "collection", collection, "entity", entityID, "version", version, "err", err)
	}
}
