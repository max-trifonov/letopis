package rules

import (
	"errors"
	"testing"
)

// ruleErrorField asserts err is a *RuleError naming the given field.
func ruleErrorField(t *testing.T, err error, field string) {
	t.Helper()
	var re *RuleError
	if !errors.As(err, &re) {
		t.Fatalf("error = %v, want *RuleError", err)
	}
	if re.Field != field {
		t.Fatalf("RuleError.Field = %q, want %q", re.Field, field)
	}
}

func TestCompileErrors(t *testing.T) {
	tests := []struct {
		name  string
		cond  Condition
		field string
	}{
		{"unknown field", Condition{Field: "amount", Op: OpEq, Value: "x"}, "condition.field"},
		{"unknown operator", Condition{Field: FieldOp, Op: "approx", Value: "x"}, "condition.op"},
		{"bad regex", Condition{Field: FieldSource, Op: OpRegex, Value: "("}, "condition.regex"},
		{"regex non-string", Condition{Field: FieldSource, Op: OpRegex, Value: 5}, "condition.regex"},
		{"in non-list", Condition{Field: FieldOp, Op: OpIn, Value: "x"}, "condition.in"},
		{"numeric non-numeric value", Condition{Field: FieldEntityID, Op: OpGt, Value: "abc"}, "condition.gt"},
		{"empty condition", Condition{}, "condition"},
		{"ambiguous condition", Condition{Field: FieldOp, Op: OpEq, Value: "u", Match: &Match{Path: "x"}}, "condition"},
		{"empty glob segment", Condition{Match: &Match{Path: "items..price"}}, "condition.match.path"},
		{"bad change op", Condition{Match: &Match{Path: "x", Op: "swap"}}, "condition.match.op"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Compile(tt.cond)
			ruleErrorField(t, err, tt.field)
		})
	}
}

// A compile error inside a combinator surfaces with the indexed path so a
// client can locate the offending clause.
func TestCompileErrorPathInList(t *testing.T) {
	cond := Condition{All: []Condition{
		scalar(FieldOp, OpEq, "update"),
		{Field: "bogus", Op: OpEq, Value: "x"},
	}}
	_, err := Compile(cond)
	ruleErrorField(t, err, "condition.all[1].field")
}

func TestValidateRule(t *testing.T) {
	valid := Rule{
		Name:      "alert",
		Condition: scalar(FieldOp, OpEq, "update"),
		Actions:   []Action{{Type: ActionLog, Log: &LogAction{Level: "warn"}}},
	}
	if err := ValidateRule(valid); err != nil {
		t.Fatalf("valid rule rejected: %v", err)
	}

	tests := []struct {
		name  string
		rule  Rule
		field string
	}{
		{
			"empty name",
			Rule{Condition: scalar(FieldOp, OpEq, "u"), Actions: valid.Actions},
			"name",
		},
		{
			"no actions",
			Rule{Name: "r", Condition: scalar(FieldOp, OpEq, "u")},
			"actions",
		},
		{
			"webhook without url",
			Rule{Name: "r", Condition: scalar(FieldOp, OpEq, "u"), Actions: []Action{{Type: ActionWebhook, Webhook: &Webhook{}}}},
			"actions[0].webhook.url",
		},
		{
			"unknown action type",
			Rule{Name: "r", Condition: scalar(FieldOp, OpEq, "u"), Actions: []Action{{Type: "email"}}},
			"actions[0].type",
		},
		{
			"bad condition propagates",
			Rule{Name: "r", Condition: Condition{Field: "x", Op: OpEq}, Actions: valid.Actions},
			"condition.field",
		},
		{
			"negative retry",
			Rule{Name: "r", Condition: scalar(FieldOp, OpEq, "u"), Actions: []Action{{Type: ActionWebhook, Webhook: &Webhook{URL: "http://x", Retry: Retry{MaxAttempts: -1}}}}},
			"actions[0].webhook.retry.max_attempts",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ruleErrorField(t, ValidateRule(tt.rule), tt.field)
		})
	}
}
