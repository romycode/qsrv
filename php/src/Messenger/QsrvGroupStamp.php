<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Messenger;

use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Sends the message in a qsrv ordering group: messages with the same group
 * are delivered strictly in order, one at a time.
 *
 *     $bus->dispatch($message, [new QsrvGroupStamp('order-'.$orderId)]);
 */
final class QsrvGroupStamp implements StampInterface
{
    public function __construct(public readonly string $groupId)
    {
        if ('' === $groupId || \strlen($groupId) > 128) {
            throw new \InvalidArgumentException('qsrv: group id must be 1 to 128 bytes');
        }
    }
}
