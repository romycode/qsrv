// Package api holds the JSON wire types shared by the server and clients.
package api

import (
	"encoding/json"
	"time"
	"unicode/utf8"
)

// Duration marshals as a Go duration string ("30s", "1m30s").
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}

type RetryPolicy struct {
	MaxAttempts    int      `json:"max_attempts"`
	InitialBackoff Duration `json:"initial_backoff"`
	MaxBackoff     Duration `json:"max_backoff"`
	Multiplier     float64  `json:"multiplier"`
}

type QueueConfig struct {
	VisibilityTimeout Duration     `json:"visibility_timeout,omitempty"`
	Retry             *RetryPolicy `json:"retry,omitempty"`
	DeadLetterQueue   string       `json:"dead_letter_queue,omitempty"`
	Ordering          string       `json:"ordering,omitempty"` // "available" (default) | "strict"
	MaxMessages       int          `json:"max_messages,omitempty"`
	// Retention of processed messages for replay. nil = server default (90 days),
	// "0s" = keep forever.
	Retention *Duration `json:"retention,omitempty"`
}

type Stats struct {
	Messages int `json:"messages"`
	Ready    int `json:"ready"`
	Delayed  int `json:"delayed"`
	InFlight int `json:"in_flight"`
	Groups   int `json:"groups"`
	History  int `json:"history"` // processed messages retained for replay
}

type Queue struct {
	Name string `json:"name"`
	QueueConfig
	Stats Stats `json:"stats"`
}

type QueueList struct {
	Queues []string `json:"queues"`
}

// SendRequest carries the body either as UTF-8 text (Body) or raw bytes
// (BodyBase64). Exactly one must be set.
type SendRequest struct {
	Body       *string `json:"body,omitempty"`
	BodyBase64 []byte  `json:"body_base64,omitempty"`
	GroupID    string  `json:"group_id,omitempty"`
	// Delay makes the message deliverable only after this long (max 720h).
	Delay Duration `json:"delay,omitempty"`
}

// Message: Body is set when the payload is valid UTF-8, BodyBase64 otherwise.
type Message struct {
	ID         string    `json:"id"`
	GroupID    string    `json:"group_id,omitempty"`
	Body       *string   `json:"body,omitempty"`
	BodyBase64 []byte    `json:"body_base64,omitempty"`
	Attempts   int       `json:"attempts"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	ReplayOf   string    `json:"replay_of,omitempty"` // original id if this is a replayed copy
}

// Bytes returns the payload regardless of how it was encoded.
func (m Message) Bytes() []byte {
	if m.Body != nil {
		return []byte(*m.Body)
	}
	return m.BodyBase64
}

// EncodeBody picks the most readable lossless encoding for b.
func EncodeBody(b []byte) (text *string, raw []byte) {
	if utf8.Valid(b) {
		s := string(b)
		return &s, nil
	}
	return nil, b
}

type Delivery struct {
	Message   Message   `json:"message"`
	LeaseID   string    `json:"lease_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// NackRequest is the optional body of a nack.
type NackRequest struct {
	// DeadLetter skips the retry policy: the message goes to the dead letter
	// queue now (dropped if the queue has none).
	DeadLetter bool `json:"dead_letter,omitempty"`
}

type ExtendRequest struct {
	By Duration `json:"by,omitempty"` // 0 = queue visibility timeout
}

type ExtendResponse struct {
	ExpiresAt time.Time `json:"expires_at"`
}

// ReplayRequest re-enqueues processed messages of the source queue into To,
// in original order, as new messages (new id, same body and group_id).
type ReplayRequest struct {
	To      string     `json:"to"`
	States  []string   `json:"states,omitempty"` // acked, dropped, dead_lettered; empty = all
	From    *time.Time `json:"from,omitempty"`   // enqueued_at >= from
	Until   *time.Time `json:"until,omitempty"`  // enqueued_at <  until
	GroupID string     `json:"group_id,omitempty"`
	Cursor  uint64     `json:"cursor,omitempty"` // next_cursor from the previous page
	Limit   int        `json:"limit,omitempty"`  // default 1000, max 10000
}

type ReplayResponse struct {
	Replayed   int    `json:"replayed"`
	NextCursor uint64 `json:"next_cursor,omitempty"` // absent = done
}

// Topic fans published messages out to its subscribed queues.
type Topic struct {
	Name   string   `json:"name"`
	Queues []string `json:"queues"`
}

type TopicList struct {
	Topics []Topic `json:"topics"`
}

// TopicRequest creates a topic, optionally with initial subscriptions.
type TopicRequest struct {
	Queues []string `json:"queues,omitempty"`
}

// PublishedMessage is the copy of a published message in one queue.
type PublishedMessage struct {
	Queue string `json:"queue"`
	Message
}

// PublishResponse lists one copy per subscribed queue (empty with none).
type PublishResponse struct {
	Messages []PublishedMessage `json:"messages"`
}

type Error struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// Error codes returned in Error.Code.
const (
	CodeBadRequest    = "bad_request"
	CodeUnauthorized  = "unauthorized"
	CodeNotFound      = "not_found"
	CodeLeaseGone     = "lease_gone"
	CodeExists        = "already_exists"
	CodeInvalidConfig = "invalid_config"
	CodeQueueFull     = "queue_full"
	CodeStorageFull   = "storage_full" // server low on disk: retry later
	CodeUnavailable   = "unavailable"
	CodeInternal      = "internal"
)
