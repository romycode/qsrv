<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Messenger;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/** Added to received envelopes: the lease the transport acks, rejects or extends. */
final class QsrvReceivedStamp implements NonSendableStampInterface
{
    public function __construct(
        public readonly string $leaseId,
        public readonly string $messageId,
        /** qsrv delivery count (1 on first delivery); independent of Messenger's RedeliveryStamp. */
        public readonly int $attempts,
        public readonly ?string $groupId,
        public readonly \DateTimeImmutable $leaseExpiresAt,
    ) {
    }
}
