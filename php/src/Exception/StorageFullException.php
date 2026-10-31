<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Exception;

/**
 * The server is low on disk space and refuses new messages, queues and
 * topics for now (receive and ack keep working): retry later.
 */
final class StorageFullException extends ApiException
{
}
