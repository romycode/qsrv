<?php

declare(strict_types=1);

namespace Example;

/** A Messenger message: a plain object, serialized to JSON on the wire. */
final class SendInvoice
{
    public function __construct(
        public readonly int $orderId,
        public readonly string $customer,
    ) {
    }
}
