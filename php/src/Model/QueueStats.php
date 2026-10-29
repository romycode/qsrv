<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

final readonly class QueueStats
{
    public function __construct(
        /** Live messages: ready + delayed + in flight + blocked behind their group. */
        public int $messages,
        public int $ready,
        public int $delayed,
        public int $inFlight,
        public int $groups,
        /** Processed messages retained for replay. */
        public int $history,
    ) {
    }

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            messages: (int) ($data['messages'] ?? 0),
            ready: (int) ($data['ready'] ?? 0),
            delayed: (int) ($data['delayed'] ?? 0),
            inFlight: (int) ($data['in_flight'] ?? 0),
            groups: (int) ($data['groups'] ?? 0),
            history: (int) ($data['history'] ?? 0),
        );
    }
}
