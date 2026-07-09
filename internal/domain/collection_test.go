package domain

import (
	"errors"
	"reflect"
	"testing"
)

func TestWithDefaultsEmpty(t *testing.T) {
	got := WithDefaults(CollectionConfig{Name: "crm.deals"})
	want := CollectionConfig{
		Name:              "crm.deals",
		ReliabilityMode:   ReliabilityDurable,
		SnapshotInterval:  DefaultSnapshotInterval,
		Retention:         Retention{Type: RetentionForever},
		MaxEventSizeBytes: DefaultMaxEventSizeBytes,
		FirstEventOp:      FirstEventCreate,
		Ordering:          Ordering{Mode: OrderingReceived},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults mismatch:\n got=%+v\nwant=%+v", got, want)
	}
}

func TestWithDefaultsPreservesSetFields(t *testing.T) {
	in := CollectionConfig{
		Name:             "crm.deals",
		ReliabilityMode:  ReliabilityStrict,
		FirstEventOp:     FirstEventUpdate,
		SnapshotInterval: 500,
		Ordering:         Ordering{Mode: OrderingSource, ReorderWindowMS: 3000},
		Retention:        Retention{Type: RetentionDays, Days: 365},
	}
	got := WithDefaults(in)
	if got.ReliabilityMode != ReliabilityStrict {
		t.Errorf("reliability overwritten: %q", got.ReliabilityMode)
	}
	if got.FirstEventOp != FirstEventUpdate {
		t.Errorf("first_event_op overwritten: %q", got.FirstEventOp)
	}
	if got.SnapshotInterval != 500 {
		t.Errorf("snapshot_interval overwritten: %d", got.SnapshotInterval)
	}
	if got.Ordering.Mode != OrderingSource || got.Ordering.ReorderWindowMS != 3000 {
		t.Errorf("ordering overwritten: %+v", got.Ordering)
	}
	if got.Retention.Type != RetentionDays || got.Retention.Days != 365 {
		t.Errorf("retention overwritten: %+v", got.Retention)
	}
}

// WithDefaults must not mutate the caller's value (it operates on a copy).
func TestWithDefaultsDoesNotMutateInput(t *testing.T) {
	in := CollectionConfig{Name: "x"}
	_ = WithDefaults(in)
	if in.ReliabilityMode != "" || in.FirstEventOp != "" {
		t.Fatal("WithDefaults mutated its input")
	}
}

func TestEnumPredicates(t *testing.T) {
	if !ValidReliabilityMode(ReliabilityFast) || ValidReliabilityMode("loud") {
		t.Error("ValidReliabilityMode")
	}
	if !ValidFirstEventOp(FirstEventUpdate) || ValidFirstEventOp("delete") {
		t.Error("ValidFirstEventOp")
	}
	// source is reserved but valid on write (ADR-011).
	if !ValidOrderingMode(OrderingSource) || ValidOrderingMode("random") {
		t.Error("ValidOrderingMode")
	}
	if !ValidRetentionType(RetentionVersions) || ValidRetentionType("never") {
		t.Error("ValidRetentionType")
	}
	if !ValidFailMode(FailClosed) || ValidFailMode("ajar") {
		t.Error("ValidFailMode")
	}
	// The empty string is "unset", not a member of any enum.
	if ValidReliabilityMode("") || ValidFirstEventOp("") || ValidOrderingMode("") || ValidRetentionType("") || ValidFailMode("") {
		t.Error("empty string must not validate as a known enum value")
	}
}

func TestCollectionConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		cfg       CollectionConfig
		wantField string // "" means valid
	}{
		{name: "empty config (all unset) is valid", cfg: CollectionConfig{Name: "crm.deals"}},
		{
			name: "fully specified valid config",
			cfg: CollectionConfig{
				ReliabilityMode:   ReliabilityStrict,
				SnapshotInterval:  50,
				Retention:         Retention{Type: RetentionDays, Days: 365},
				MaxEventSizeBytes: 2048,
				FirstEventOp:      FirstEventUpdate,
				Ordering:          Ordering{Mode: OrderingSource, ReorderWindowMS: 3000},
				Plugins:           map[string]PluginConfig{"hash_chain": {Enabled: true, FailMode: FailClosed}},
			},
		},
		{name: "bad reliability_mode", cfg: CollectionConfig{ReliabilityMode: "loud"}, wantField: "reliability_mode"},
		{name: "bad first_event_op", cfg: CollectionConfig{FirstEventOp: "delete"}, wantField: "first_event_op"},
		{name: "bad ordering.mode", cfg: CollectionConfig{Ordering: Ordering{Mode: "random"}}, wantField: "ordering.mode"},
		{name: "bad retention.type", cfg: CollectionConfig{Retention: Retention{Type: "never"}}, wantField: "retention.type"},
		{name: "negative snapshot_interval", cfg: CollectionConfig{SnapshotInterval: -1}, wantField: "snapshot_interval"},
		{name: "negative max_event_size_bytes", cfg: CollectionConfig{MaxEventSizeBytes: -1}, wantField: "max_event_size_bytes"},
		{
			name:      "bad plugin fail_mode",
			cfg:       CollectionConfig{Plugins: map[string]PluginConfig{"hash_chain": {FailMode: "ajar"}}},
			wantField: "plugins.hash_chain.fail_mode",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantField == "" {
				if err != nil {
					t.Fatalf("want valid, got %v", err)
				}
				return
			}
			var ce *ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("want *ConfigError, got %T (%v)", err, err)
			}
			if ce.Field != tc.wantField {
				t.Fatalf("field = %q, want %q", ce.Field, tc.wantField)
			}
		})
	}
}
