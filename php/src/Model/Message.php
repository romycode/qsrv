<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

final readonly class Message
{
    public function __construct(
        public string $id,
        /** Raw payload bytes, whatever encoding the server used on the wire. */
        public string $body,
        public ?string $groupId,
        /** Deliveries so far (1 on the first delivery). */
        public int $attempts,
        public \DateTimeImmutable $enqueuedAt,
        /** Id of the original message when this one is a replayed copy. */
        public ?string $replayOf,
    ) {
    }

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): self
    {
        if (isset($data['body'])) {
            $body = (string) $data['body'];
        } elseif (isset($data['body_base64'])) {
            $body = base64_decode((string) $data['body_base64'], true);
            if (false === $body) {
                throw new \UnexpectedValueException('qsrv: invalid body_base64');
            }
        } else {
            $body = '';
        }

        return new self(
            id: (string) $data['id'],
            body: $body,
            groupId: isset($data['group_id']) && '' !== $data['group_id'] ? (string) $data['group_id'] : null,
            attempts: (int) ($data['attempts'] ?? 0),
            enqueuedAt: Time::parse($data['enqueued_at'] ?? null),
            replayOf: isset($data['replay_of']) && '' !== $data['replay_of'] ? (string) $data['replay_of'] : null,
        );
    }
}
