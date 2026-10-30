<?php

declare(strict_types=1);

namespace Romycode\Qsrv;

use Romycode\Qsrv\Exception\ApiException;
use Romycode\Qsrv\Exception\TransportException;
use Romycode\Qsrv\Model\Delivery;
use Romycode\Qsrv\Model\Message;
use Romycode\Qsrv\Model\Queue;
use Romycode\Qsrv\Model\QueueConfig;
use Romycode\Qsrv\Model\ReplayResult;
use Romycode\Qsrv\Model\Time;
use Romycode\Qsrv\Model\Topic;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Contracts\HttpClient\Exception\ExceptionInterface as HttpException;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * HTTP client for qsrv. Safe to share; one instance per server is enough.
 *
 *     $qsrv = new Client('http://localhost:8080', getenv('QSRV_API_KEY'));
 *     $qsrv->send('emails', json_encode($payload), groupId: 'user-42');
 *     if ($d = $qsrv->receive('emails', wait: 20)) {
 *         handle($d->message->body);
 *         $qsrv->ack('emails', $d->leaseId);
 *     }
 */
final class Client
{
    /** Server-side cap on long-poll waits, in seconds. */
    public const MAX_WAIT = 20;
    /** Server-side cap on send delays, in seconds (720h). */
    public const MAX_DELAY = 720 * 3600;

    private readonly HttpClientInterface $http;
    private readonly string $baseUrl;

    public function __construct(
        string $baseUrl,
        #[\SensitiveParameter] private readonly string $apiKey,
        ?HttpClientInterface $http = null,
        /** Seconds; long-poll receives add their wait on top. */
        private readonly float $timeout = 10,
    ) {
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->http = $http ?? HttpClient::create();
    }

    /** @return list<string> */
    public function listQueues(): array
    {
        $out = $this->request('GET', '/v1/queues');

        return array_values(array_map('strval', $out['queues'] ?? []));
    }

    /** @throws Exception\QueueExistsException if it already exists */
    public function createQueue(string $name, QueueConfig $config = new QueueConfig()): Queue
    {
        return Queue::fromArray($this->request('PUT', self::q($name), (object) $config->toArray()));
    }

    /** Config and current stats. @throws Exception\NotFoundException */
    public function getQueue(string $name): Queue
    {
        return Queue::fromArray($this->request('GET', self::q($name)));
    }

    /** Deletes the queue, its live messages and its history. */
    public function deleteQueue(string $name): void
    {
        $this->request('DELETE', self::q($name));
    }

    /**
     * Enqueues $body (any bytes). Returns once the message is on disk.
     *
     * @param string|null $groupId messages with the same group are delivered strictly in order, one at a time
     * @param float       $delay   seconds until the message can be delivered (max 720h)
     *
     * @throws Exception\QueueFullException when max_messages is reached
     */
    public function send(string $queue, string $body, ?string $groupId = null, float $delay = 0): Message
    {
        $out = Message::fromArray($this->request('POST', self::q($queue, '/messages'), self::payload($body, $groupId, $delay)));

        return $out->withBody($body);
    }

    /**
     * Leases the next message. With $wait > 0 it long-polls up to $wait seconds
     * (max 20) and returns as soon as a message arrives. Null when empty.
     */
    public function receive(string $queue, float $wait = 0): ?Delivery
    {
        $wait = max(0, min($wait, self::MAX_WAIT));
        $path = self::q($queue, '/receive');
        if ($wait > 0) {
            $path .= '?wait='.Time::duration($wait);
        }
        $out = $this->request('POST', $path, null, $this->timeout + $wait);

        return null === $out ? null : Delivery::fromArray($out);
    }

    /** Marks the message processed. @throws Exception\LeaseGoneException if the lease expired */
    public function ack(string $queue, string $leaseId): void
    {
        $this->request('POST', self::q($queue, '/leases/'.rawurlencode($leaseId).'/ack'));
    }

    /**
     * Gives the message back; the queue's retry policy decides when it is
     * redelivered. With $deadLetter it skips the retries and goes to the
     * dead letter queue now (dropped if the queue has none).
     */
    public function nack(string $queue, string $leaseId, bool $deadLetter = false): void
    {
        $this->request('POST', self::q($queue, '/leases/'.rawurlencode($leaseId).'/nack'), $deadLetter ? ['dead_letter' => true] : null);
    }

    /**
     * Heartbeat for long jobs: the lease expires $by seconds from now
     * (null = the queue's visibility timeout). Returns the new expiry.
     */
    public function extend(string $queue, string $leaseId, ?float $by = null): \DateTimeImmutable
    {
        $in = null === $by ? null : ['by' => Time::duration($by)];
        $out = $this->request('POST', self::q($queue, '/leases/'.rawurlencode($leaseId).'/extend'), $in);

        return Time::parse($out['expires_at'] ?? null);
    }

    /**
     * Re-enqueues processed messages of $queue into $to, in original order,
     * one page per call. Loop while nextCursor is not null.
     *
     * @param list<string>|null $states acked, dropped, dead_lettered (null = all)
     */
    public function replay(
        string $queue,
        string $to,
        ?array $states = null,
        ?\DateTimeInterface $from = null,
        ?\DateTimeInterface $until = null,
        ?string $groupId = null,
        ?int $cursor = null,
        ?int $limit = null,
    ): ReplayResult {
        $in = array_filter([
            'to' => $to,
            'states' => $states,
            'from' => $from?->format(\DATE_RFC3339_EXTENDED),
            'until' => $until?->format(\DATE_RFC3339_EXTENDED),
            'group_id' => $groupId,
            'cursor' => $cursor,
            'limit' => $limit,
        ], static fn ($v) => null !== $v && '' !== $v);
        $out = $this->request('POST', self::q($queue, '/replay'), $in);

        return new ReplayResult((int) ($out['replayed'] ?? 0), isset($out['next_cursor']) ? (int) $out['next_cursor'] : null);
    }

    // --- Topics: fan-out to several queues ------------------------------

    /** @return list<Topic> */
    public function listTopics(): array
    {
        $out = $this->request('GET', '/v1/topics');

        return array_map(Topic::fromArray(...), $out['topics'] ?? []);
    }

    /**
     * @param list<string> $queues initial subscriptions (queues must exist)
     *
     * @throws Exception\QueueExistsException if the topic already exists
     */
    public function createTopic(string $name, array $queues = []): Topic
    {
        return Topic::fromArray($this->request('PUT', self::t($name), (object) ([] === $queues ? [] : ['queues' => array_values($queues)])));
    }

    /** @throws Exception\NotFoundException */
    public function getTopic(string $name): Topic
    {
        return Topic::fromArray($this->request('GET', self::t($name)));
    }

    /** Deletes the topic. Its queues and their messages stay. */
    public function deleteTopic(string $name): void
    {
        $this->request('DELETE', self::t($name));
    }

    /** $queue gets a copy of every message published to $topic from now on. Idempotent. */
    public function subscribe(string $topic, string $queue): Topic
    {
        return Topic::fromArray($this->request('PUT', self::t($topic, '/subscriptions/'.rawurlencode($queue))));
    }

    /** Stops the copies to $queue. Idempotent; messages already queued stay. */
    public function unsubscribe(string $topic, string $queue): Topic
    {
        return Topic::fromArray($this->request('DELETE', self::t($topic, '/subscriptions/'.rawurlencode($queue))));
    }

    /**
     * Inserts a copy of $body into every queue subscribed to $topic, in one
     * transaction: all of them get it or none does. Group and delay apply in
     * each queue as for send().
     *
     * @return array<string, Message> the copy in each queue, by queue name (empty without subscribers)
     *
     * @throws Exception\QueueFullException if any subscribed queue is full (nothing is published)
     */
    public function publish(string $topic, string $body, ?string $groupId = null, float $delay = 0): array
    {
        $out = $this->request('POST', self::t($topic, '/messages'), self::payload($body, $groupId, $delay));
        $copies = [];
        foreach ($out['messages'] ?? [] as $m) {
            $copies[(string) $m['queue']] = Message::fromArray($m)->withBody($body);
        }

        return $copies;
    }

    /** True if /healthz answers 200. Never throws. */
    public function healthy(): bool
    {
        try {
            return 200 === $this->http->request('GET', $this->baseUrl.'/healthz', ['timeout' => $this->timeout])->getStatusCode();
        } catch (HttpException) {
            return false;
        }
    }

    /**
     * @return array<string, mixed>|null decoded JSON, null for 204
     *
     * @throws ApiException|TransportException
     */
    private function request(string $method, string $path, array|object|null $json = null, ?float $timeout = null): ?array
    {
        $options = [
            'headers' => ['Authorization' => 'Bearer '.$this->apiKey, 'Accept' => 'application/json'],
            'timeout' => $timeout ?? $this->timeout,
            'max_duration' => ($timeout ?? $this->timeout) + 5,
        ];
        if (null !== $json) {
            $options['json'] = $json;
        }

        try {
            $response = $this->http->request($method, $this->baseUrl.$path, $options);
            $status = $response->getStatusCode();
            $content = $response->getContent(false);
        } catch (HttpException $e) {
            throw new TransportException(\sprintf('qsrv: %s %s: %s', $method, $path, $e->getMessage()), 0, $e);
        }

        if (204 === $status) {
            return null;
        }
        $data = '' === $content ? [] : json_decode($content, true);
        if ($status >= 400) {
            if (!\is_array($data)) { // not from qsrv: a proxy or load balancer error page
                throw ApiException::from($status, 'http_'.$status, substr(trim(strip_tags($content)), 0, 200));
            }
            throw ApiException::from($status, (string) ($data['code'] ?? 'http_'.$status), (string) ($data['error'] ?? ''));
        }
        if (!\is_array($data)) {
            throw new TransportException(\sprintf('qsrv: %s %s: invalid JSON response (HTTP %d)', $method, $path, $status));
        }

        return $data;
    }

    /** @return array<string, string> */
    private static function payload(string $body, ?string $groupId, float $delay): array
    {
        if ($delay < 0 || $delay > self::MAX_DELAY) {
            throw new \InvalidArgumentException(\sprintf('qsrv: delay must be between 0 and %d seconds', self::MAX_DELAY));
        }
        $in = 1 === preg_match('//u', $body) ? ['body' => $body] : ['body_base64' => base64_encode($body)];
        if (null !== $groupId && '' !== $groupId) {
            $in['group_id'] = $groupId;
        }
        if ($delay > 0) {
            $in['delay'] = Time::duration($delay);
        }

        return $in;
    }

    private static function t(string $topic, string $suffix = ''): string
    {
        return '/v1/topics/'.rawurlencode($topic).$suffix;
    }

    private static function q(string $queue, string $suffix = ''): string
    {
        return '/v1/queues/'.rawurlencode($queue).$suffix;
    }
}
