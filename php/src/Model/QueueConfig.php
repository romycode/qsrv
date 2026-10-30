<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

/**
 * Queue settings. Null fields use the server default. Durations in seconds.
 */
final readonly class QueueConfig
{
    public const ORDERING_AVAILABLE = 'available';
    public const ORDERING_STRICT = 'strict';

    public function __construct(
        /** How long a received message stays leased before it is redelivered. */
        public ?float $visibilityTimeout = null,
        public ?RetryPolicy $retry = null,
        /** Queue that receives messages that exhausted their retries. */
        public ?string $deadLetterQueue = null,
        /** "available" (default) or "strict" (whole queue is one ordering group). */
        public ?string $ordering = null,
        /** Max live messages; sends beyond it fail with QueueFullException. 0 = unlimited. */
        public ?int $maxMessages = null,
        /** How long processed messages are kept for replay. 0 = forever. */
        public ?float $retention = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return array_filter([
            'visibility_timeout' => null === $this->visibilityTimeout ? null : Time::duration($this->visibilityTimeout),
            'retry' => $this->retry?->toArray(),
            'dead_letter_queue' => $this->deadLetterQueue,
            'ordering' => $this->ordering,
            'max_messages' => $this->maxMessages,
            'retention' => null === $this->retention ? null : Time::duration($this->retention),
        ], static fn ($v) => null !== $v);
    }

    /** @param array<string, mixed> $data API or YAML-style keys (snake_case) */
    public static function fromArray(array $data): self
    {
        $dur = static function (mixed $v): ?float {
            return match (true) {
                null === $v => null,
                \is_int($v), \is_float($v) => (float) $v,
                default => Time::seconds($v),
            };
        };

        return new self(
            visibilityTimeout: $dur($data['visibility_timeout'] ?? null),
            retry: match (true) {
                false === ($data['retry'] ?? null), 'false' === ($data['retry'] ?? null) => RetryPolicy::none(),
                \is_array($data['retry'] ?? null) => RetryPolicy::fromArray($data['retry']),
                default => null,
            },
            deadLetterQueue: isset($data['dead_letter_queue']) && '' !== $data['dead_letter_queue'] ? (string) $data['dead_letter_queue'] : null,
            ordering: isset($data['ordering']) && '' !== $data['ordering'] ? (string) $data['ordering'] : null,
            maxMessages: isset($data['max_messages']) ? (int) $data['max_messages'] : null,
            retention: $dur($data['retention'] ?? null),
        );
    }

    /**
     * The settings this config sets explicitly that differ from $actual,
     * as name => [configured, actual] in readable form.
     *
     * @return array<string, array{string, string}>
     */
    public function diff(self $actual): array
    {
        $dur = static fn (?float $s): string => null === $s ? 'default' : rtrim(rtrim(\sprintf('%.3f', $s), '0'), '.').'s';
        $out = [];
        $cmp = static function (string $name, mixed $want, mixed $have, callable $fmt) use (&$out): void {
            if (null === $want) {
                return; // not configured: whatever the queue has is fine
            }
            $same = \is_float($want) || \is_float($have) ? abs((float) $want - (float) $have) < 0.0005 : $want === $have;
            if (!$same) {
                $out[$name] = [$fmt($want), $fmt($have)];
            }
        };
        $str = static fn (mixed $v): string => null === $v ? 'none' : (string) $v;

        $cmp('visibility_timeout', $this->visibilityTimeout, $actual->visibilityTimeout, $dur);
        $cmp('dead_letter_queue', $this->deadLetterQueue, $actual->deadLetterQueue, $str);
        $cmp('ordering', $this->ordering, $actual->ordering ?? self::ORDERING_AVAILABLE, $str);
        $cmp('max_messages', $this->maxMessages, $actual->maxMessages ?? 0, $str);
        $cmp('retention', $this->retention, $actual->retention, $dur);
        if (null !== $this->retry) {
            $have = $actual->retry ?? new RetryPolicy(0);
            $cmp('retry.max_attempts', $this->retry->maxAttempts, $have->maxAttempts, $str);
            $cmp('retry.initial_backoff', $this->retry->initialBackoff, $have->initialBackoff, $dur);
            $cmp('retry.max_backoff', $this->retry->maxBackoff, $have->maxBackoff, $dur);
            $cmp('retry.multiplier', $this->retry->multiplier, $have->multiplier, $str);
        }

        return $out;
    }
}
