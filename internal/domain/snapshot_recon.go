package domain

import "github.com/max-trifonov/letopis/internal/diff"

// ShouldSnapshot reports whether a write at this version should materialize a snapshot:
// every interval-th version, per collection. A non-positive interval disables snapshots.
func ShouldSnapshot(version int64, interval int) bool {
	if interval <= 0 || version <= 0 {
		return false
	}
	return version%int64(interval) == 0
}

// Reconstruct replays an ordered event tail onto a base state. base is never mutated.
// baseDeleted=true means the snapshot was taken at a delete: the retained value is
// still displayed, but the next diff applies against {} (reincarnation). The same reset
// happens at a delete mid-tail, keeping a snapshot-anchored replay identical to a full
// genesis replay across delete/recreate cycles.
func Reconstruct(base map[string]any, baseDeleted bool, events []Event) (map[string]any, bool, error) {
	// work: what the next diff applies against.
	// report: what we return for the version reached.
	// They diverge at a delete: report is the pre-delete value, work resets to {}.
	work := emptyIfNil(base)
	if baseDeleted {
		work = map[string]any{}
	}
	report := emptyIfNil(base)
	deleted := baseDeleted

	for _, ev := range events {
		if ev.Op == OpDelete {
			report = work
			work = map[string]any{}
			deleted = true
			continue
		}
		applied, err := diff.Apply(work, ev.Changes)
		if err != nil {
			return nil, false, err
		}
		m, _ := applied.(map[string]any)
		if m == nil {
			m = map[string]any{}
		}
		work, report, deleted = m, m, false
	}
	return report, deleted, nil
}

// emptyIfNil normalizes a missing state to an empty object — the neutral base a
// genesis replay or a reincarnation after delete builds on.
func emptyIfNil(s map[string]any) map[string]any {
	if s == nil {
		return map[string]any{}
	}
	return s
}
