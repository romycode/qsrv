<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

final readonly class Queue
{
    public function __construct(
        public string $name,
        public QueueConfig $config,
        public QueueStats $stats,
    ) {
    }

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            name: (string) $data['name'],
            config: QueueConfig::fromArray($data),
            stats: QueueStats::fromArray($data['stats'] ?? []),
        );
    }
}
