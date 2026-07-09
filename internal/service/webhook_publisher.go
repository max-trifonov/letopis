package service

import (
	"context"
	"fmt"

	"github.com/max-trifonov/letopis/internal/delivery"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// WebhookPublisher implements DeliveryPublisher over the delivery queue: builds
// the signed body and enqueues a delivery.Task. The HTTP call happens in the
// dispatcher, never on the write path.
type WebhookPublisher struct {
	q queue.Queue
}

func NewWebhookPublisher(q queue.Queue) *WebhookPublisher {
	return &WebhookPublisher{q: q}
}

var _ DeliveryPublisher = (*WebhookPublisher)(nil)

// Publish builds and enqueues a webhook delivery task. The tenant travels in the
// envelope so the dispatcher can write DLQ entries without re-auth.
func (p *WebhookPublisher) Publish(ctx context.Context, req DeliveryRequest) error {
	principal, ok := tenant.FromContext(ctx)
	if !ok {
		return tenant.ErrNoKey
	}
	body, err := delivery.BuildBody(req.Collection, req.Event, req.RuleID, req.RuleName)
	if err != nil {
		return fmt.Errorf("service: build webhook body: %w", err)
	}
	task := delivery.Task{
		Tenant: delivery.TenantRef{
			ID:     principal.Tenant.ID,
			DBURI:  principal.Tenant.Database.URI,
			DBName: principal.Tenant.Database.Name,
		},
		DeliveryID:  domain.NewDeliveryID(),
		RuleID:      req.RuleID,
		RuleName:    req.RuleName,
		Collection:  req.Collection,
		URL:         req.Webhook.URL,
		SecretRef:   req.Webhook.SecretRef,
		TimeoutMS:   req.Webhook.TimeoutMS,
		MaxAttempts: req.Webhook.Retry.MaxAttempts,
		Attempt:     0,
		Body:        body,
	}
	payload, err := delivery.EncodeTask(task)
	if err != nil {
		return fmt.Errorf("service: encode delivery task: %w", err)
	}
	// Shard by rule id for parallelism; deliveries are unordered between
	// themselves, unlike entity events.
	return p.q.Publish(ctx, queue.Message{Key: req.RuleID, Payload: payload})
}

// Requeue re-enqueues a parked dead letter. Reuses the stored signed body and
// resets the attempt counter; the delivery id is preserved for receiver-side
// deduplication on X-HM-Delivery.
func (p *WebhookPublisher) Requeue(ctx context.Context, dl domain.DeadLetter) error {
	principal, ok := tenant.FromContext(ctx)
	if !ok {
		return tenant.ErrNoKey
	}
	task := delivery.Task{
		Tenant: delivery.TenantRef{
			ID:     principal.Tenant.ID,
			DBURI:  principal.Tenant.Database.URI,
			DBName: principal.Tenant.Database.Name,
		},
		DeliveryID: dl.DeliveryID,
		RuleID:     dl.RuleID,
		Collection: dl.Collection,
		URL:        dl.URL,
		SecretRef:  dl.SecretRef,
		Attempt:    0, // restart the retry schedule
		Body:       dl.Body,
	}
	payload, err := delivery.EncodeTask(task)
	if err != nil {
		return fmt.Errorf("service: encode redelivery task: %w", err)
	}
	return p.q.Publish(ctx, queue.Message{Key: dl.RuleID, Payload: payload})
}
