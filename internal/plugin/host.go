package plugin

import (
	"context"

	"github.com/max-trifonov/letopis/internal/domain"
)

// Host is the resolved set of plugins an instance runs: ordered pre-store and
// post-store chains and an action map keyed by Type. A nil Host is a valid no-op,
// so the core runs unchanged when no plugins are registered.
type Host struct {
	pre     []PreStorePlugin
	post    []PostStorePlugin
	actions map[string]ActionPlugin
	metrics Metrics
}

// NewHost builds a host from the registered plugins. A later registration for the
// same action type wins.
func NewHost(pre []PreStorePlugin, post []PostStorePlugin, actions []ActionPlugin, m Metrics) *Host {
	am := make(map[string]ActionPlugin, len(actions))
	for _, a := range actions {
		am[a.Type()] = a
	}
	return &Host{pre: pre, post: post, actions: am, metrics: m}
}

// HasPreStore reports whether any pre-store plugin is enabled for the collection.
// The write path calls it to decide whether to incur the extra last-event read
// pre-store needs, so a collection without plugins pays nothing.
func (h *Host) HasPreStore(cfg *domain.CollectionConfig) bool {
	if h == nil || cfg == nil {
		return false
	}
	for _, p := range h.pre {
		if pc, ok := cfg.Plugins[p.Name()]; ok && pc.Enabled {
			return true
		}
	}
	return false
}

// RunPreStore runs the enabled pre-store plugins for the collection in order.
// A fail-closed plugin error aborts and is returned as *FailClosedError (the event
// is not committed); a fail-open error is counted and skipped, so the event is written
// without that plugin's contribution. An unset fail mode defaults to fail-open.
func (h *Host) RunPreStore(ctx context.Context, cfg *domain.CollectionConfig, collection string, ev *domain.Event, prev EntityView) error {
	if h == nil || cfg == nil {
		return nil
	}
	var draft *EventDraft
	for _, p := range h.pre {
		pc, ok := cfg.Plugins[p.Name()]
		if !ok || !pc.Enabled {
			continue
		}
		if draft == nil {
			draft = newDraft(collection, ev)
		}
		if err := p.PreStore(ctx, draft, prev); err != nil {
			if pc.FailMode == domain.FailClosed {
				return &FailClosedError{Plugin: p.Name(), Err: err}
			}
			if h.metrics != nil {
				h.metrics.IncPluginError(p.Name(), "pre_store")
			}
		}
	}
	return nil
}

// RunPostStore runs the enabled post-store plugins after a durable commit, best-effort.
// The event is already stored, so nothing here can fail the write.
func (h *Host) RunPostStore(ctx context.Context, cfg *domain.CollectionConfig, ev domain.Event) {
	if h == nil || cfg == nil {
		return
	}
	for _, p := range h.post {
		pc, ok := cfg.Plugins[p.Name()]
		if !ok || !pc.Enabled {
			continue
		}
		p.PostStore(ctx, ev)
	}
}

// Action resolves a rule-action type to its plugin, or reports no plugin handles it.
func (h *Host) Action(typ string) (ActionPlugin, bool) {
	if h == nil {
		return nil, false
	}
	a, ok := h.actions[typ]
	return a, ok
}
