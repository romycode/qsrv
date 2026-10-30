package queue

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Topics fan messages out to queues. Publishing to a topic inserts one copy
// of the message into every subscribed queue in a single transaction: all
// subscribers get it or none does. Each copy is an ordinary message of its
// queue, with that queue's retries, DLQ, ordering groups, retention and
// replay; consumers don't know it came from a topic.

var (
	ErrTopicExists   = errors.New("topic: already exists")
	ErrTopicNotFound = errors.New("topic: not found")
)

// MaxSubscriptions caps the queues per topic (each publish writes one row each).
const MaxSubscriptions = 64

type Topic struct {
	Name   string
	Queues []string // sorted
}

// Published is the copy of a published message in one subscribed queue.
type Published struct {
	Queue   string
	Message Message
}

func (b *Broker) CreateTopic(name string, queues []string) (Topic, error) {
	if !validFullName(name) {
		return Topic{}, fmt.Errorf("%w: invalid topic name %q", ErrInvalidConfig, name)
	}
	subs, err := normalizeSubs(queues)
	if err != nil {
		return Topic{}, err
	}
	for _, q := range subs {
		if !sameTenant(name, q) {
			return Topic{}, crossTenant("queue", name, q)
		}
	}
	b.persist.RLock()
	b.mu.Lock()
	if _, ok := b.topics[name]; ok {
		b.mu.Unlock()
		b.persist.RUnlock()
		return Topic{}, ErrTopicExists
	}
	if err := b.queuesExistLocked(subs); err != nil {
		b.mu.Unlock()
		b.persist.RUnlock()
		return Topic{}, err
	}
	b.topics[name] = subs
	pos := b.store.append(op{kind: opTopicSet, queue: name, subs: subs, at: b.clock.Now()})
	b.mu.Unlock()
	b.persist.RUnlock()

	b.store.waitSync(pos)
	return Topic{Name: name, Queues: slices.Clone(subs)}, nil
}

func (b *Broker) GetTopic(name string) (Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	subs, ok := b.topics[name]
	if !ok {
		return Topic{}, ErrTopicNotFound
	}
	return Topic{Name: name, Queues: slices.Clone(subs)}, nil
}

// Topics returns every topic, sorted by name.
func (b *Broker) Topics() []Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Topic, 0, len(b.topics))
	for name, subs := range b.topics {
		out = append(out, Topic{Name: name, Queues: slices.Clone(subs)})
	}
	slices.SortFunc(out, func(x, y Topic) int { return cmp.Compare(x.Name, y.Name) })
	return out
}

// DeleteTopic removes the topic. Subscribed queues and their messages stay.
func (b *Broker) DeleteTopic(name string) error {
	b.persist.RLock()
	b.mu.Lock()
	if _, ok := b.topics[name]; !ok {
		b.mu.Unlock()
		b.persist.RUnlock()
		return ErrTopicNotFound
	}
	delete(b.topics, name)
	pos := b.store.append(op{kind: opTopicDelete, queue: name})
	b.mu.Unlock()
	b.persist.RUnlock()

	b.store.waitSync(pos)
	return nil
}

// Subscribe adds a queue to the topic. Idempotent. The queue gets messages
// published from now on, not earlier ones.
func (b *Broker) Subscribe(topic, queue string) (Topic, error) {
	return b.updateSubs(topic, func(subs []string) ([]string, error) {
		if slices.Contains(subs, queue) {
			return subs, nil
		}
		if !sameTenant(topic, queue) {
			return nil, crossTenant("queue", topic, queue)
		}
		if _, ok := b.queues[queue]; !ok {
			return nil, fmt.Errorf("%w: queue %q does not exist", ErrInvalidConfig, queue)
		}
		return normalizeSubs(append(slices.Clone(subs), queue))
	})
}

// Unsubscribe removes a queue from the topic. Idempotent. Messages already
// in the queue stay.
func (b *Broker) Unsubscribe(topic, queue string) (Topic, error) {
	return b.updateSubs(topic, func(subs []string) ([]string, error) {
		return slices.DeleteFunc(slices.Clone(subs), func(q string) bool { return q == queue }), nil
	})
}

func (b *Broker) updateSubs(topic string, fn func([]string) ([]string, error)) (Topic, error) {
	b.persist.RLock()
	b.mu.Lock()
	subs, ok := b.topics[topic]
	if !ok {
		b.mu.Unlock()
		b.persist.RUnlock()
		return Topic{}, ErrTopicNotFound
	}
	next, err := fn(subs)
	if err != nil {
		b.mu.Unlock()
		b.persist.RUnlock()
		return Topic{}, err
	}
	var pos uint64
	if !slices.Equal(next, subs) {
		b.topics[topic] = next
		pos = b.store.append(op{kind: opTopicSet, queue: topic, subs: next, at: b.clock.Now()})
	}
	b.mu.Unlock()
	b.persist.RUnlock()

	b.store.waitSync(pos)
	return Topic{Name: topic, Queues: slices.Clone(next)}, nil
}

// Publish inserts a copy of body into every queue subscribed to the topic,
// atomically: if any queue refuses it (full, closing) none gets it. A topic
// with no subscriptions accepts the message and drops it. groupID and delay
// apply in each queue as for Send.
func (b *Broker) Publish(topic string, body []byte, groupID string, delay time.Duration) ([]Published, error) {
	if err := validateSend(groupID, delay); err != nil {
		return nil, err
	}
	body = append([]byte(nil), body...)

	b.persist.RLock()
	b.mu.RLock()
	subs, ok := b.topics[topic]
	queues := make([]*Queue, 0, len(subs))
	for _, name := range subs { // sorted: a fixed lock order across publishers
		if q := b.queues[name]; q != nil {
			queues = append(queues, q)
		}
	}
	b.mu.RUnlock()
	if !ok {
		b.persist.RUnlock()
		return nil, ErrTopicNotFound
	}

	for _, q := range queues {
		q.mu.Lock()
	}
	unlock := func() {
		for i := len(queues) - 1; i >= 0; i-- {
			queues[i].mu.Unlock()
		}
		b.persist.RUnlock()
	}
	for _, q := range queues {
		if err := q.acceptLocked(1); err != nil {
			unlock()
			return nil, fmt.Errorf("queue %q: %w", q.name, err)
		}
	}

	now := b.clock.Now()
	msgs := make([]*Message, len(queues))
	ops := make([]op, len(queues))
	for i, q := range queues {
		m, rec := q.newMessageLocked(body, groupID, delay, now)
		msgs[i], ops[i] = m, op{kind: opInsert, queue: q.name, msg: rec}
	}
	pos := b.store.appendAll(ops) // one transaction
	out := make([]Published, len(queues))
	for i, q := range queues {
		q.addLocked(msgs[i])
		out[i] = Published{Queue: q.name, Message: msgs[i].public()}
		out[i].Message.Body = body
	}
	unlock()

	b.store.waitSync(pos)
	for _, q := range queues {
		b.hooks.Event(q.name, EvSent, 1)
	}
	return out, nil
}

// topicsOfLocked lists the topics a queue is subscribed to. Caller holds b.mu.
func (b *Broker) topicsOfLocked(queue string) []string {
	var out []string
	for name, subs := range b.topics {
		if slices.Contains(subs, queue) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func (b *Broker) queuesExistLocked(names []string) error {
	for _, n := range names {
		if _, ok := b.queues[n]; !ok {
			return fmt.Errorf("%w: queue %q does not exist", ErrInvalidConfig, n)
		}
	}
	return nil
}

func normalizeSubs(queues []string) ([]string, error) {
	subs := slices.Clone(queues)
	slices.Sort(subs)
	subs = slices.Compact(subs)
	if len(subs) > MaxSubscriptions {
		return nil, fmt.Errorf("%w: at most %d queues per topic", ErrInvalidConfig, MaxSubscriptions)
	}
	if subs == nil {
		subs = []string{}
	}
	return subs, nil
}
