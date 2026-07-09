package domain

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// ULIDs are used for server-issued ids: they sort lexicographically by creation
// time, so nodes cluster naturally and ids are roughly k-ordered without a
// coordinator. 26-char Crockford-base32 encoder, no external dependency.

// crockford is the ULID alphabet (Crockford base32, excluding I, L, O, U).
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ID prefixes keep the id spaces visually distinct in logs and client code.
const (
	activityIDPrefix = "act_"
	flowIDPrefix     = "f_"
	ticketIDPrefix   = "tkt_"
	auditIDPrefix    = "aud_"
	ruleIDPrefix     = "rule_"
	deadLetterPrefix = "dlq_"
	deliveryIDPrefix = "dlv_"
)

func NewActivityID() string    { return activityIDPrefix + newULID() }
func NewFlowID() string        { return flowIDPrefix + newULID() }
func NewTicketID() string      { return ticketIDPrefix + newULID() }
func NewAuditID() string       { return auditIDPrefix + newULID() }
func NewRuleID() string        { return ruleIDPrefix + newULID() }
func NewDeadLetterID() string  { return deadLetterPrefix + newULID() }

// NewDeliveryID is stable across every retry; travels in X-HM-Delivery so
// a receiver can deduplicate at-least-once retries.
func NewDeliveryID() string { return deliveryIDPrefix + newULID() }

// newULID builds a 26-character ULID: a 48-bit millisecond timestamp followed
// by 80 bits of randomness, encoded in Crockford base32. crypto/rand cannot
// realistically fail here; if it ever does we still produce a syntactically
// valid (all-zero-entropy) id rather than panic on a hot ingest path.
func newULID() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	_, _ = rand.Read(b[6:])
	return encodeULID(b)
}

// encodeULID renders the 128-bit value as 26 Crockford-base32 characters.
// The 130 bits the 26 chars can hold are zero-padded at the top two bits,
// matching the canonical ULID layout.
func encodeULID(b [16]byte) string {
	// Pack into two 64-bit halves to shift bits across byte boundaries cheaply.
	hi := binary.BigEndian.Uint64(b[0:8])
	lo := binary.BigEndian.Uint64(b[8:16])

	var out [26]byte
	for i := 25; i >= 0; i-- {
		out[i] = crockford[lo&0x1f]
		// Carry the low 5 bits of hi down into lo as we consume lo.
		lo = lo>>5 | (hi&0x1f)<<59
		hi >>= 5
	}
	return string(out[:])
}
