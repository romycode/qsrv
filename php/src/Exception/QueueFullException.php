<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Exception;

/** The queue reached max_messages (backpressure): retry later. */
final class QueueFullException extends ApiException
{
}
