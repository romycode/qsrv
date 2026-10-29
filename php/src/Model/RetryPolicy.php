<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

/** Server-side redelivery policy for nacked or expired messages. Durations in seconds. */
final readonly class RetryPolicy
{
    public function __construct(
        public int $maxAttempts,
        public float $initialBackoff = 0,
        public float $maxBackoff = 0,
        /** < 1 means constant backoff. */
        public float $multiplier = 1,
    ) {
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

        return new self(
            maxAttempts: (int) ($data['max_attempts'] ?? 0),
            initialBackoff: $dur($data['initial_backoff'] ?? 0),
            maxBackoff: $dur($data['max_backoff'] ?? 0),
            multiplier: (float) ($data['multiplier'] ?? 1),
        );
    }
}
