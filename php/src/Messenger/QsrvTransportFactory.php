<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Messenger;

use Romycode\Qsrv\Client;
use Romycode\Qsrv\Model\QueueConfig;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * DSN: qsrv://API_KEY@host:8080/queue (http) or qsrvs://API_KEY@host/queue (https).
 *
 * Options (DSN query string or transport `options`):
 *   queue         queue name, if not in the DSN path (optional with topic:
 *                 then the transport only publishes)
 *   topic         publish to this topic on send, instead of to the queue
 *   subscribe     topics the queue subscribes to on setup (list, or comma
 *                 separated in the DSN)
 *   api_key        API key, if not in the DSN user part
 *   native_retries who retries failed messages (default true):
 *                    true   qsrv: queue_config.retry and dead_letter_queue;
 *                           Messenger's retry_strategy is not used
 *                    false  Symfony Messenger: retry_strategy and
 *                           failure_transport, like other transports
 *   wait_time      seconds get() long-polls, 0-20 (default 5)
 *   auto_setup     create the queue on first use if missing (default true)
 *   timeout        HTTP timeout in seconds (default 10)
 *   queue_config   settings the queue is created with (`options` only):
 *                  visibility_timeout, retry (false = no retries, or
 *                  {max_attempts, initial_backoff, max_backoff, multiplier}),
 *                  dead_letter_queue, ordering, max_messages, retention.
 *                  Durations as "30s" or seconds. qsrv can't change an
 *                  existing queue: setup fails if they differ.
 *
 * Register it as a service tagged messenger.transport_factory.
 */
final class QsrvTransportFactory implements TransportFactoryInterface
{
    private const OPTIONS = ['queue', 'api_key', 'native_retries', 'wait_time', 'auto_setup', 'timeout', 'queue_config', 'topic', 'subscribe'];

    public function __construct(private readonly ?HttpClientInterface $httpClient = null)
    {
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'qsrv://') || str_starts_with($dsn, 'qsrvs://');
    }

    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        // Messenger passes the transport name and other framework keys; they are not ours.
        unset($options['transport_name']);
        $c = self::parse($dsn, $options);

        return new QsrvTransport(
            client: new Client($c['url'], $c['api_key'], $this->httpClient, $c['timeout']),
            queue: $c['queue'],
            serializer: $serializer,
            waitTime: $c['wait_time'],
            autoSetup: $c['auto_setup'],
            nativeRetries: $c['native_retries'],
            queueConfig: $c['queue_config'],
            topic: $c['topic'],
            subscribe: $c['subscribe'],
        );
    }

    /**
     * @param array<string, mixed> $options
     *
     * @return array{url: string, api_key: string, queue: string, wait_time: float, auto_setup: bool, native_retries: bool, timeout: float, queue_config: QueueConfig, topic: ?string, subscribe: list<string>}
     *
     * @internal
     */
    public static function parse(#[\SensitiveParameter] string $dsn, array $options = []): array
    {
        $parts = parse_url($dsn);
        if (false === $parts || !isset($parts['scheme'], $parts['host'])) {
            throw new InvalidArgumentException('qsrv: invalid DSN, expected qsrv://API_KEY@host:port/queue');
        }
        $query = [];
        parse_str($parts['query'] ?? '', $query);
        $all = $options + $query;

        if (\array_key_exists('on_reject', $all)) {
            throw new InvalidArgumentException('qsrv: "on_reject" was replaced by "native_retries": use native_retries: true for qsrv retries (was on_reject: nack) or false for Symfony retries (was on_reject: ack)');
        }
        if ($unknown = array_diff(array_keys($all), self::OPTIONS)) {
            throw new InvalidArgumentException(\sprintf('qsrv: unknown option(s) "%s"; supported: %s', implode('", "', $unknown), implode(', ', self::OPTIONS)));
        }

        $scheme = 'qsrvs' === $parts['scheme'] ? 'https' : 'http';
        $url = $scheme.'://'.$parts['host'].(isset($parts['port']) ? ':'.$parts['port'] : '');

        $key = $all['api_key'] ?? null;
        foreach (['user', 'pass'] as $p) {
            if ((null === $key || '' === $key) && isset($parts[$p])) {
                $key = rawurldecode($parts[$p]);
            }
        }
        if (null === $key || '' === $key) {
            throw new InvalidArgumentException('qsrv: missing API key (qsrv://KEY@host/queue or the api_key option)');
        }

        $topic = isset($all['topic']) && '' !== $all['topic'] ? (string) $all['topic'] : null;
        $subscribe = $all['subscribe'] ?? [];
        if (\is_string($subscribe)) {
            $subscribe = array_map('trim', explode(',', $subscribe));
        }
        if (!\is_array($subscribe)) {
            throw new InvalidArgumentException('qsrv: subscribe must be a list of topics');
        }
        $subscribe = array_values(array_filter(array_map('strval', $subscribe), static fn ($t) => '' !== $t));

        $queue = $all['queue'] ?? rawurldecode(trim($parts['path'] ?? '', '/'));
        if ('' === $queue && null === $topic) {
            throw new InvalidArgumentException('qsrv: missing queue name (qsrv://KEY@host/QUEUE, the queue option, or a topic to publish to)');
        }
        if ('' === $queue && [] !== $subscribe) {
            throw new InvalidArgumentException('qsrv: subscribe needs a queue');
        }

        $wait = (float) ($all['wait_time'] ?? 5);
        if ($wait < 0 || $wait > Client::MAX_WAIT) {
            throw new InvalidArgumentException(\sprintf('qsrv: wait_time must be between 0 and %d', Client::MAX_WAIT));
        }

        $autoSetup = self::bool($all, 'auto_setup', true);
        $nativeRetries = self::bool($all, 'native_retries', true);

        $qc = $all['queue_config'] ?? [];
        if (!\is_array($qc)) {
            throw new InvalidArgumentException('qsrv: queue_config must be an array (use transport options, not the DSN)');
        }

        return [
            'url' => $url,
            'api_key' => (string) $key,
            'queue' => (string) $queue,
            'wait_time' => $wait,
            'auto_setup' => $autoSetup,
            'native_retries' => $nativeRetries,
            'timeout' => (float) ($all['timeout'] ?? 10),
            'queue_config' => QueueConfig::fromArray($qc),
            'topic' => $topic,
            'subscribe' => $subscribe,
        ];
    }

    /** @param array<string, mixed> $all */
    private static function bool(array $all, string $key, bool $default): bool
    {
        $v = filter_var($all[$key] ?? $default, \FILTER_VALIDATE_BOOL, \FILTER_NULL_ON_FAILURE);
        if (null === $v) {
            throw new InvalidArgumentException(\sprintf('qsrv: %s must be true or false', $key));
        }

        return $v;
    }
}
