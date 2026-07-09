// Package mongo implements the storage ports over MongoDB 7+: a connection
// manager that resolves the per-tenant database from the request context, plus
// the event, current-state and collection-config repositories. No business logic
// lives here — only the domain↔BSON mapping.
package mongo

import (
	"fmt"
	"regexp"
	"strings"
)

// Physical collection prefixes. A logical collection "crm.deals" maps to
// ev_crm_deals / cur_crm_deals / sn_crm_deals.
const (
	prefixEvents  = "ev_"
	prefixCurrent = "cur_"
	prefixSnap    = "sn_"

	// CollectionsConfig is the per-tenant collection-config store.
	CollectionsConfig = "_collections"

	// Rules is the per-tenant rules store. One collection holds all rules for the
	// tenant; each document carries the logical collection it belongs to, so the
	// rule cache warms with a single ListAll.
	Rules = "_rules"

	// DLQ is the per-tenant dead-letter store for undelivered webhooks. One
	// collection holds every rule's dead letters; each document carries its rule_id
	// and collection for filtering.
	DLQ = "_dlq"

	// FlowActivities is the per-tenant activity store. The double-underscore marks
	// it a system collection, distinct from user collections under the ev_ prefix.
	FlowActivities = "ev__flow"

	// SystemEvents is the per-tenant administrative audit log: config changes, key
	// actions, etc. Like ev__flow it is a system collection under the double-
	// underscore prefix, so the stats fan-out skips it.
	SystemEvents = "ev__system"
)

// logicalName bounds what a tenant may name a collection: dot-separated
// segments of [a-z0-9]. Underscore is deliberately excluded — we flatten '.'
// to '_' for physical names, so allowing '_' in logical names would let
// "crm.deals" and "crm_deals" collide on one physical collection. The rule
// also rejects anything that could escape into another collection or an
// operator-injection shape.
var logicalName = regexp.MustCompile(`^[a-z0-9]+(?:\.[a-z0-9]+)*$`)

// validateLogical guards collection names taken from request paths before
// they become physical collection names.
func validateLogical(name string) error {
	if name == "" {
		return fmt.Errorf("mongo: empty collection name")
	}
	if !logicalName.MatchString(name) {
		return fmt.Errorf("mongo: invalid collection name %q (allowed: lowercase letters, digits and '.')", name)
	}
	return nil
}

// physicalName joins a prefix with the dot-flattened logical name. It is
// deterministic and total over names that pass validateLogical.
func physicalName(prefix, logical string) string {
	return prefix + strings.ReplaceAll(logical, ".", "_")
}

func eventsCollection(logical string) string  { return physicalName(prefixEvents, logical) }
func currentCollection(logical string) string { return physicalName(prefixCurrent, logical) }
func snapCollection(logical string) string    { return physicalName(prefixSnap, logical) }

// logicalFromEvents reverses eventsCollection for the flow fan-out, which only
// has physical ev_* names to work with. The mapping is total and unambiguous:
// logical names never contain '_' (validateLogical), so every '_' after the
// prefix was a flattened '.'.
func logicalFromEvents(physical string) string {
	return strings.ReplaceAll(strings.TrimPrefix(physical, prefixEvents), "_", ".")
}

// isUserEventCollection reports whether a physical name is a user ev_*
// collection, excluding system collections under the ev__ (double underscore)
// prefix such as ev__flow and ev__system. The fan-out reads only user
// collections.
func isUserEventCollection(name string) bool {
	return strings.HasPrefix(name, prefixEvents) && !strings.HasPrefix(name, prefixEvents+"_")
}
