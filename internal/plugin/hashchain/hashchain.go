// Package hashchain is the per-entity cryptographic hash-chain plugin. For each
// event it computes:
//
//	hash = "sha256:" + hex(SHA-256(prev_hash ‖ canonical(event)))
//
// where prev_hash is the previous event's hash (the collection genesis for the
// first link), and canonical(event) is a deterministic projection of the event's
// signed fields.
//
// The projection and the genesis formula are the verification contract: :verify
// and verify-chain rebuild them byte for byte from stored events, so they live
// here as the single source of truth and are reused unchanged by verification.
// Pure logic, no I/O: reads via EventDraft/EntityView, writes via SetIntegrity.
package hashchain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
)

// Name is the plugin's stable identifier, matched against the key under
// CollectionConfig.Plugins ("hash_chain").
const Name = "hash_chain"

// genesisPrefix binds the genesis hash to the collection name (so a link cannot
// be transplanted between collections) and carries a scheme version: a future
// change to the canonical projection bumps "v1", starting a distinct chain space
// rather than silently invalidating stored chains.
const genesisPrefix = "letopis:genesis:v1:"

const hashScheme = "sha256:"

// Plugin is the hash-chain pre-store plugin. Stateless — one instance serves
// every tenant and collection.
type Plugin struct{}

func New() *Plugin { return &Plugin{} }

var _ plugin.PreStorePlugin = (*Plugin)(nil)

func (*Plugin) Name() string { return Name }

// PreStore computes the entity's next chain link and records it on the draft.
// prev_hash comes from the last event's Integrity; when the entity has no prior
// link (its first event ever, or the first after the plugin was enabled mid-history)
// it is the collection genesis, starting a fresh chain from this point.
func (p *Plugin) PreStore(_ context.Context, draft *plugin.EventDraft, prev plugin.EntityView) error {
	prevHash := Genesis(draft.Collection())
	if prev.LastEvent != nil && prev.LastEvent.Integrity != nil && prev.LastEvent.Integrity.Hash != "" {
		prevHash = prev.LastEvent.Integrity.Hash
	}
	hash, err := Hash(prevHash, projectionFromDraft(draft))
	if err != nil {
		return err
	}
	draft.SetIntegrity(hash, prevHash)
	return nil
}

// Projection is the exact set of event fields that enter the hash — the verification
// contract. Server-set fields (ts_stored, request_id, ip/meta) are deliberately
// excluded. Version is also excluded: it is assigned by storage after this hook,
// and chain continuity is carried by prev_hash alone, so leaving version out keeps
// the link reproducible regardless of the append retry.
// author/source/ts_source are client-supplied and stable on replay, so they are signed.
type Projection struct {
	Op       domain.EntityOp
	EntityID string
	Author   string
	Source   string
	TSSource time.Time
	Changes  []diff.Change
	Flow     *domain.Flow
}

// ProjectionFromEvent builds the canonical projection of a stored event. It is
// the verification entry point: :verify reads an event back and rebuilds the
// projection through this function, so the field set never diverges between write
// and verify.
func ProjectionFromEvent(ev domain.Event) Projection {
	return Projection{
		Op:       ev.Op,
		EntityID: ev.EntityID,
		Author:   ev.Author,
		Source:   ev.Source,
		TSSource: ev.TSSource,
		Changes:  ev.Changes,
		Flow:     ev.Flow,
	}
}

func projectionFromDraft(d *plugin.EventDraft) Projection {
	return Projection{
		Op:       d.Op(),
		EntityID: d.EntityID(),
		Author:   d.Author(),
		Source:   d.Source(),
		TSSource: d.TSSource(),
		Changes:  d.Changes(),
		Flow:     d.Flow(),
	}
}

// Genesis is the deterministic first prev_hash of a collection's chains:
// "sha256:" + hex(SHA-256("letopis:genesis:v1:" + collection)).
func Genesis(collection string) string {
	sum := sha256.Sum256([]byte(genesisPrefix + collection))
	return hashScheme + hex.EncodeToString(sum[:])
}

// Hash computes a chain link: "sha256:" + hex(SHA-256(prev_hash ‖ canonical(p))).
// prev_hash is hashed as its full string form ("sha256:..."), so a verifier
// reconstructs the input from the stored PrevHash verbatim.
func Hash(prevHash string, p Projection) (string, error) {
	canon, err := Canonical(p)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(prevHash))
	h.Write(canon)
	return hashScheme + hex.EncodeToString(h.Sum(nil)), nil
}

// Canonical returns the deterministic byte encoding the hash is taken over.
// Only present fields contribute: an absent optional field — no author, empty
// changes (a delete), no flow block — adds nothing, so introducing a new optional
// field later does not change the canonical form of events that omit it, and
// existing chains keep verifying. ts_source is truncated to millisecond precision
// (the resolution BSON datetime round-trips at), so a hash computed at write time
// matches one recomputed from the stored event at verify time.
func Canonical(p Projection) ([]byte, error) {
	m := map[string]any{
		"op":        string(p.Op),
		"entity_id": p.EntityID,
	}
	if len(p.Changes) > 0 {
		m["changes"] = canonicalChanges(p.Changes)
	}
	if p.Author != "" {
		m["author"] = p.Author
	}
	if p.Source != "" {
		m["source"] = p.Source
	}
	if !p.TSSource.IsZero() {
		m["ts_source"] = p.TSSource.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
	}
	if p.Flow != nil {
		m["flow"] = canonicalFlow(p.Flow)
	}
	return diff.Canonicalize(m)
}

// canonicalChanges projects the diff into present-only maps, preserving the change
// order (stable, produced by the diff engine). diff.Canonicalize sorts each map's
// keys, so the field order within a change does not matter.
func canonicalChanges(cs []diff.Change) []any {
	out := make([]any, len(cs))
	for i, c := range cs {
		m := map[string]any{"path": c.Path, "op": string(c.Op)}
		switch c.Op {
		case diff.OpAdd:
			m["new"] = c.New
		case diff.OpRemove:
			m["old"] = c.Old
		default:
			m["old"], m["new"] = c.Old, c.New
		}
		out[i] = m
	}
	return out
}

func canonicalFlow(f *domain.Flow) map[string]any {
	m := map[string]any{}
	if f.ID != "" {
		m["id"] = f.ID
	}
	if f.Step != "" {
		m["step"] = f.Step
	}
	if len(f.CausedBy) > 0 {
		refs := make([]any, len(f.CausedBy))
		for i, r := range f.CausedBy {
			refs[i] = canonicalRef(r)
		}
		m["caused_by"] = refs
	}
	return m
}

func canonicalRef(r domain.FlowRef) map[string]any {
	m := map[string]any{}
	if r.ActivityID != "" {
		m["activity_id"] = r.ActivityID
	}
	if r.Collection != "" {
		m["collection"] = r.Collection
	}
	if r.EntityID != "" {
		m["entity_id"] = r.EntityID
	}
	if r.Version != 0 {
		m["version"] = r.Version
	}
	if r.EventID != "" {
		m["event_id"] = r.EventID
	}
	return m
}
