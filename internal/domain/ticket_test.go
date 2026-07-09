package domain

import "testing"

func TestTicketCanTransition(t *testing.T) {
	cases := []struct {
		from, to TicketStatus
		want     bool
	}{
		{TicketAccepted, TicketProcessing, true},
		{TicketAccepted, TicketStored, true}, // strict-ish: a fast worker may settle without a visible processing step
		{TicketAccepted, TicketFailed, true},
		{TicketProcessing, TicketStored, true},
		{TicketProcessing, TicketFailed, true},
		// Illegal: a terminal state never moves, in particular a reclaimed
		// delivery must not pull stored back to processing.
		{TicketStored, TicketProcessing, false},
		{TicketStored, TicketFailed, false},
		{TicketFailed, TicketProcessing, false},
		{TicketProcessing, TicketAccepted, false},
		// Re-entering the same status is not a transition.
		{TicketProcessing, TicketProcessing, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%s, %s) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestTicketTerminal(t *testing.T) {
	for s, want := range map[TicketStatus]bool{
		TicketAccepted:   false,
		TicketProcessing: false,
		TicketStored:     true,
		TicketFailed:     true,
	} {
		if got := s.Terminal(); got != want {
			t.Errorf("%s.Terminal() = %v, want %v", s, got, want)
		}
	}
}
