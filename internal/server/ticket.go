package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/max-trifonov/letopis/internal/domain"
)

// An out-of-scope ticket id is indistinguishable from not-found.
type TicketReadService interface {
	Get(ctx context.Context, id string) (*domain.Ticket, error)
}

type ticketHandlers struct {
	svc TicketReadService
}

type ticketResponse struct {
	TicketID         string `json:"ticket_id"`
	Status           string `json:"status"`
	EntityCollection string `json:"entity_collection,omitempty"`
	EntityID         string `json:"entity_id,omitempty"`
	Error            string `json:"error,omitempty"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
}

func (h ticketHandlers) get(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "ticketId")
	if id == "" {
		writeError(w, http.StatusBadRequest, "ticket id is required")
		return
	}
	t, err := h.svc.Get(r.Context(), id)
	switch {
	case errors.Is(err, domain.ErrTicketNotFound):
		// Unknown, expired, or another tenant's ticket — all 404, leaking nothing.
		writeError(w, http.StatusNotFound, "ticket not found")
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, ticketResponse{
		TicketID:         t.ID,
		Status:           string(t.Status),
		EntityCollection: t.EntityCollection,
		EntityID:         t.EntityID,
		Error:            t.Error,
		CreatedAt:        t.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:        t.UpdatedAt.UTC().Format(time.RFC3339),
	})
}
