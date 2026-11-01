// Package client is a Go SDK for qsrv.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/romycode/qsrv/pkg/api"
)

// APIError is any non-2xx response.
type APIError struct {
	Status int
	Code   string
	Msg    string
}

func (e *APIError) Error() string { return fmt.Sprintf("qsrv: %d %s: %s", e.Status, e.Code, e.Msg) }

// IsLeaseGone reports whether the lease expired or was already settled. The
// message may have been redelivered: the consumer must be idempotent.
func IsLeaseGone(err error) bool { return hasCode(err, api.CodeLeaseGone) }
func IsNotFound(err error) bool  { return hasCode(err, api.CodeNotFound) }
func IsQueueFull(err error) bool { return hasCode(err, api.CodeQueueFull) }

// IsStorageFull reports that the server is low on disk and refuses new
// messages for now; receive and ack keep working. Retry later.
func IsStorageFull(err error) bool { return hasCode(err, api.CodeStorageFull) }

func hasCode(err error, code string) bool {
	var e *APIError
	return errors.As(err, &e) && e.Code == code
}

type Client struct {
	base string
	key  string
	hc   *http.Client
}

type Option func(*Client)

// WithHTTPClient overrides the HTTP client. Do not set a Timeout shorter than
// the long-poll wait; use contexts instead.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }

func New(baseURL, apiKey string, opts ...Option) *Client {
	// The default transport keeps only 2 idle conns per host: with many
	// concurrent workers that means constant reconnects (and TIME_WAIT churn).
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 256
	c := &Client{base: strings.TrimRight(baseURL, "/"), key: apiKey, hc: &http.Client{Transport: tr}}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) ListQueues(ctx context.Context) ([]string, error) {
	var out api.QueueList
	err := c.do(ctx, http.MethodGet, "/v1/queues", nil, &out)
	return out.Queues, err
}

func (c *Client) CreateQueue(ctx context.Context, name string, cfg api.QueueConfig) (api.Queue, error) {
	var out api.Queue
	err := c.do(ctx, http.MethodPut, "/v1/queues/"+url.PathEscape(name), cfg, &out)
	return out, err
}

func (c *Client) GetQueue(ctx context.Context, name string) (api.Queue, error) {
	var out api.Queue
	err := c.do(ctx, http.MethodGet, "/v1/queues/"+url.PathEscape(name), nil, &out)
	return out, err
}

func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/queues/"+url.PathEscape(name), nil, nil)
}

// PeekOptions selects messages to look at without receiving them.
type PeekOptions struct {
	History bool     // processed messages (newest first) instead of live ones (oldest first)
	States  []string // live: ready, delayed, in_flight; history: acked, dropped, dead_lettered
	Cursor  uint64   // NextCursor of the previous page
	Limit   int      // default 50, max 500
}

// PeekMessages lists messages of a queue without receiving them: no lease,
// no attempt, no effect on delivery.
func (c *Client) PeekMessages(ctx context.Context, queue string, o PeekOptions) (api.MessagePage, error) {
	q := url.Values{}
	if o.History {
		q.Set("view", "history")
	}
	if len(o.States) > 0 {
		q.Set("state", strings.Join(o.States, ","))
	}
	if o.Cursor > 0 {
		q.Set("cursor", strconv.FormatUint(o.Cursor, 10))
	}
	if o.Limit > 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	p := qpath(queue, "/messages")
	if len(q) > 0 {
		p += "?" + q.Encode()
	}
	var out api.MessagePage
	err := c.do(ctx, http.MethodGet, p, nil, &out)
	return out, err
}

// PeekMessage returns one message, live or processed, with its body.
func (c *Client) PeekMessage(ctx context.Context, queue, id string) (api.MessageInfo, error) {
	var out api.MessageInfo
	err := c.do(ctx, http.MethodGet, qpath(queue, "/messages/"+url.PathEscape(id)), nil, &out)
	return out, err
}

// Overview returns every queue (live counts) and topic visible to the key.
func (c *Client) Overview(ctx context.Context) (api.Overview, error) {
	var out api.Overview
	err := c.do(ctx, http.MethodGet, "/v1/overview", nil, &out)
	return out, err
}

// Send enqueues body. groupID is optional ("" = no ordering group).
func (c *Client) Send(ctx context.Context, queue string, body []byte, groupID string) (api.Message, error) {
	return c.SendWith(ctx, queue, body, SendOptions{GroupID: groupID})
}

// SendOptions are the optional parameters of a send.
type SendOptions struct {
	GroupID string        // ordering group ("" = none)
	Delay   time.Duration // deliverable only after this long (max 720h)
	// Attributes travel with the message (up to 16, 4096 bytes in total).
	Attributes map[string]string
}

// SendWith enqueues body with options.
func (c *Client) SendWith(ctx context.Context, queue string, body []byte, o SendOptions) (api.Message, error) {
	text, raw := api.EncodeBody(body)
	var out api.Message
	in := api.SendRequest{Body: text, BodyBase64: raw, GroupID: o.GroupID, Delay: api.Duration(o.Delay), Attributes: o.Attributes}
	err := c.do(ctx, http.MethodPost, qpath(queue, "/messages"), in, &out)
	return out, err
}

// Receive long-polls up to wait (server caps it at 20s). Returns nil, nil when empty.
func (c *Client) Receive(ctx context.Context, queue string, wait time.Duration) (*api.Delivery, error) {
	p := qpath(queue, "/receive")
	if wait > 0 {
		p += "?wait=" + url.QueryEscape(wait.String())
	}
	var out api.Delivery
	found := false
	err := c.doFound(ctx, http.MethodPost, p, nil, &out, &found)
	if err != nil || !found {
		return nil, err
	}
	return &out, nil
}

func (c *Client) Ack(ctx context.Context, queue, lease string) error {
	return c.do(ctx, http.MethodPost, qpath(queue, "/leases/"+url.PathEscape(lease)+"/ack"), nil, nil)
}

func (c *Client) Nack(ctx context.Context, queue, lease string) error {
	return c.do(ctx, http.MethodPost, qpath(queue, "/leases/"+url.PathEscape(lease)+"/nack"), nil, nil)
}

// DeadLetter fails the message without retrying: it goes to the queue's dead
// letter queue now (dropped if there is none). Use it for failures that a
// retry can't fix, like a payload that can't be decoded.
func (c *Client) DeadLetter(ctx context.Context, queue, lease string) error {
	return c.do(ctx, http.MethodPost, qpath(queue, "/leases/"+url.PathEscape(lease)+"/nack"), api.NackRequest{DeadLetter: true}, nil)
}

// Extend pushes the lease deadline to now+by (0 = queue visibility timeout).
func (c *Client) Extend(ctx context.Context, queue, lease string, by time.Duration) (time.Time, error) {
	var out api.ExtendResponse
	err := c.do(ctx, http.MethodPost, qpath(queue, "/leases/"+url.PathEscape(lease)+"/extend"), api.ExtendRequest{By: api.Duration(by)}, &out)
	return out.ExpiresAt, err
}

// Replay re-enqueues one page of processed messages from queue into req.To.
// Loop while NextCursor != 0, passing it back as req.Cursor.
func (c *Client) Replay(ctx context.Context, queue string, req api.ReplayRequest) (api.ReplayResponse, error) {
	var out api.ReplayResponse
	err := c.do(ctx, http.MethodPost, qpath(queue, "/replay"), req, &out)
	return out, err
}

// --- topics ---

func tpath(topic, suffix string) string { return "/v1/topics/" + url.PathEscape(topic) + suffix }

func (c *Client) ListTopics(ctx context.Context) ([]api.Topic, error) {
	var out api.TopicList
	err := c.do(ctx, http.MethodGet, "/v1/topics", nil, &out)
	return out.Topics, err
}

// CreateTopic creates a topic subscribed to queues (may be empty).
func (c *Client) CreateTopic(ctx context.Context, name string, queues ...string) (api.Topic, error) {
	var out api.Topic
	err := c.do(ctx, http.MethodPut, tpath(name, ""), api.TopicRequest{Queues: queues}, &out)
	return out, err
}

func (c *Client) GetTopic(ctx context.Context, name string) (api.Topic, error) {
	var out api.Topic
	err := c.do(ctx, http.MethodGet, tpath(name, ""), nil, &out)
	return out, err
}

// DeleteTopic removes the topic; its queues and their messages stay.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, tpath(name, ""), nil, nil)
}

// Subscribe makes queue receive a copy of every message published to topic
// from now on. Idempotent.
func (c *Client) Subscribe(ctx context.Context, topic, queue string) (api.Topic, error) {
	var out api.Topic
	err := c.do(ctx, http.MethodPut, tpath(topic, "/subscriptions/"+url.PathEscape(queue)), nil, &out)
	return out, err
}

// Unsubscribe stops copies to queue. Idempotent; queued messages stay.
func (c *Client) Unsubscribe(ctx context.Context, topic, queue string) (api.Topic, error) {
	var out api.Topic
	err := c.do(ctx, http.MethodDelete, tpath(topic, "/subscriptions/"+url.PathEscape(queue)), nil, &out)
	return out, err
}

// Publish inserts a copy of body into every queue subscribed to topic, in one
// transaction: all of them get it or none does.
func (c *Client) Publish(ctx context.Context, topic string, body []byte, o SendOptions) ([]api.PublishedMessage, error) {
	text, raw := api.EncodeBody(body)
	var out api.PublishResponse
	in := api.SendRequest{Body: text, BodyBase64: raw, GroupID: o.GroupID, Delay: api.Duration(o.Delay), Attributes: o.Attributes}
	err := c.do(ctx, http.MethodPost, tpath(topic, "/messages"), in, &out)
	return out.Messages, err
}

func qpath(queue, suffix string) string { return "/v1/queues/" + url.PathEscape(queue) + suffix }

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	return c.doFound(ctx, method, path, in, out, nil)
}

func (c *Client) doFound(ctx context.Context, method, path string, in, out any, found *bool) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var e api.Error
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if json.Unmarshal(b, &e) != nil || e.Code == "" {
			e = api.Error{Code: http.StatusText(resp.StatusCode), Error: strings.TrimSpace(string(b))}
		}
		return &APIError{Status: resp.StatusCode, Code: e.Code, Msg: e.Error}
	}
	if resp.StatusCode == http.StatusNoContent || out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	if found != nil {
		*found = true
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
