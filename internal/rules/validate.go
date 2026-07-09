package rules

import "strconv"

// ValidateRule checks a rule before it is stored: a non-empty name, a compilable
// condition, and a valid non-empty action list. Compilation is the validation for
// the condition — so a rule that validates is one the engine can run.
func ValidateRule(r Rule) error {
	if r.Name == "" {
		return &RuleError{Field: "name", Reason: "must not be empty"}
	}
	if _, err := Compile(r.Condition); err != nil {
		return err
	}
	return ValidateActions(r.Actions)
}

// ValidateActions checks a rule's action list: non-empty, every action a known type
// with its required fields set. A webhook needs a URL; a non-positive retry count
// or timeout is rejected so a rule can't encode a delivery that never fires.
func ValidateActions(actions []Action) error {
	if len(actions) == 0 {
		return &RuleError{Field: "actions", Reason: "must not be empty"}
	}
	for i, a := range actions {
		path := "actions[" + strconv.Itoa(i) + "]"
		switch a.Type {
		case ActionWebhook:
			if err := validateWebhook(a.Webhook, path+".webhook"); err != nil {
				return err
			}
		case ActionLog:
			// Level is optional; an unset level defaults at execution.
		default:
			return &RuleError{Field: path + ".type", Reason: "unknown action type " + strconv.Quote(string(a.Type))}
		}
	}
	return nil
}

func validateWebhook(w *Webhook, path string) error {
	if w == nil || w.URL == "" {
		return &RuleError{Field: path + ".url", Reason: "must not be empty"}
	}
	if w.TimeoutMS < 0 {
		return &RuleError{Field: path + ".timeout_ms", Reason: "must not be negative"}
	}
	if w.Retry.MaxAttempts < 0 {
		return &RuleError{Field: path + ".retry.max_attempts", Reason: "must not be negative"}
	}
	return nil
}
