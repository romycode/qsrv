<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

/** A leased message: ack, nack or extend it with $leaseId before $expiresAt. */
final readonly class Delivery
{
    public function __construct(
        public Message $message,
        public string $leaseId,
        public \DateTimeImmutable $expiresAt,
    ) {
    }

    /** @param array<string, mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            message: Message::fromArray($data['message'] ?? []),
            leaseId: (string) $data['lease_id'],
            expiresAt: Time::parse($data['expires_at'] ?? null),
        );
    }
}
