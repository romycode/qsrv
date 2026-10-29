<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

/** A topic and the queues that get a copy of each message published to it. */
final readonly class Topic
{
    /** @param list<string> $queues sorted */
    public function __construct(
        public string $name,
        public array $queues,
    ) {
    }

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self((string) $data['name'], array_values(array_map('strval', $data['queues'] ?? [])));
    }
}
