package mongo

import "testing"

func TestPhysicalNames(t *testing.T) {
	if got := eventsCollection("crm.deals"); got != "ev_crm_deals" {
		t.Errorf("events name = %q, want ev_crm_deals", got)
	}
	if got := currentCollection("crm.deals"); got != "cur_crm_deals" {
		t.Errorf("current name = %q, want cur_crm_deals", got)
	}
}

func TestValidateLogical(t *testing.T) {
	valid := []string{"crm.deals", "deals", "crm.sub.deals", "a1.b2"}
	for _, n := range valid {
		if err := validateLogical(n); err != nil {
			t.Errorf("validateLogical(%q) = %v, want nil", n, err)
		}
	}
	invalid := []string{"", "Crm.Deals", "crm deals", "crm.$where", "crm..deals", ".deals", "deals.", "crm/deals", "crm_deals"}
	for _, n := range invalid {
		if err := validateLogical(n); err == nil {
			t.Errorf("validateLogical(%q) = nil, want error", n)
		}
	}
}
