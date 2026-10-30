<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

/**
 * Server-side redelivery policy for nacked or expired messages. Durations in
 * seconds. Fields left out take qsrv's defaults: 5 attempts, 1s backoff
 * doubling up to 1m.
 */
final readonly class RetryPolicy
{
    public function __construct(
        /** Total deliveries: 1 = no retries, 5 = 1 delivery + 4 retries, 0 = retry forever. */
        public int $maxAttempts = 5,
        /** Wait before the first retry; 0 = retry immediately. */
        public float $initialBackoff = 1,
        public float $maxBackoff = 60,
        /** Backoff growth per attempt; < 1 means constant. */
        public float $multiplier = 2,
    ) {
    }

    /** One delivery, no retries: a failure goes straight to the dead letter queue. */
    public static function none(): self
    {
        return new self(maxAttempts: 1);
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return [
            'max_attempts' => $this->maxAttempts,
            'initial_backoff' => Time::duration($this->initialBackoff),
            'max_backoff' => Time::duration($this->maxBackoff),
            'multiplier' => $this->multiplier,
        ];
    }

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): self
    {
        $dur = static fn (mixed $v): float => \is_int($v) || \is_float($v) ? (float) $v : (Time::seconds($v) ?? 0.0);
        $d = new self();

        return new self(
            maxAttempts: (int) ($data['max_attempts'] ?? $d->maxAttempts),
            initialBackoff: isset($data['initial_backoff']) ? $dur($data['initial_backoff']) : $d->initialBackoff,
            maxBackoff: isset($data['max_backoff']) ? $dur($data['max_backoff']) : $d->maxBackoff,
            multiplier: (float) ($data['multiplier'] ?? $d->multiplier),
        );
    }
}
