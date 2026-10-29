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
 *   queue         queue name, if not in the DSN path
 *   api_key       API key, if not in the DSN user part
 *   wait_time     seconds get() long-polls, 0-20 (default 5)
 *   auto_setup    create the queue on first use if missing (default true)
 *   on_reject     "ack" (default) or "nack": see QsrvTransport
 *   timeout       HTTP timeout in seconds (default 10)
 *   queue_config  settings used when the queue is created (`options` only):
 *                 visibility_timeout, retry{max_attempts, initial_backoff,
 *                 max_backoff, multiplier}, dead_letter_queue, ordering,
 *                 max_messages, retention. Durations as "30s" or seconds.
 *
 * Register it as a service tagged messenger.transport_factory.
 */
final class QsrvTransportFactory implements TransportFactoryInterface
{
    private const OPTIONS = ['queue', 'api_key', 'wait_time', 'auto_setup', 'on_reject', 'timeout', 'queue_config'];

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
            onReject: $c['on_reject'],
            queueConfig: $c['queue_config'],
        );
    }

    /**
     * @param array<string, mixed> $options
     *
     * @return array{url: string, api_key: string, queue: string, wait_time: float, auto_setup: bool, on_reject: string, timeout: float, queue_config: QueueConfig}
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

        $queue = $all['queue'] ?? rawurldecode(trim($parts['path'] ?? '', '/'));
        if ('' === $queue) {
            throw new InvalidArgumentException('qsrv: missing queue name (qsrv://KEY@host/QUEUE or the queue option)');
        }

        $wait = (float) ($all['wait_time'] ?? 5);
        if ($wait < 0 || $wait > Client::MAX_WAIT) {
            throw new InvalidArgumentException(\sprintf('qsrv: wait_time must be between 0 and %d', Client::MAX_WAIT));
        }

        $autoSetup = filter_var($all['auto_setup'] ?? true, \FILTER_VALIDATE_BOOL, \FILTER_NULL_ON_FAILURE);
        if (null === $autoSetup) {
            throw new InvalidArgumentException('qsrv: auto_setup must be a boolean');
        }

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
            'on_reject' => (string) ($all['on_reject'] ?? QsrvTransport::ON_REJECT_ACK),
            'timeout' => (float) ($all['timeout'] ?? 10),
            'queue_config' => QueueConfig::fromArray($qc),
        ];
    }
}
