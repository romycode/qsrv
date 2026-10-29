<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Messenger;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/** Added on send by a transport with a `topic`: the copy created in each subscribed queue. */
final class QsrvPublishedStamp implements NonSendableStampInterface
{
    /** @param array<string, string> $messageIds message id by queue name (empty without subscribers) */
    public function __construct(
        public readonly string $topic,
        public readonly array $messageIds,
    ) {
    }
}
