<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Exception;

/**
 * The server answered with an error. $errorCode is the API code
 * (not_found, lease_gone, queue_full, ...), $status the HTTP status.
 */
class ApiException extends \RuntimeException implements QsrvException
{
    public function __construct(
        public readonly int $status,
        public readonly string $errorCode,
        string $message,
    ) {
        parent::__construct(\sprintf('qsrv: %d %s: %s', $status, $errorCode, $message), $status);
    }

    public static function from(int $status, string $errorCode, string $message): self
    {
        return match ($errorCode) {
            'not_found' => new NotFoundException($status, $errorCode, $message),
            'lease_gone' => new LeaseGoneException($status, $errorCode, $message),
            'already_exists' => new QueueExistsException($status, $errorCode, $message),
            'queue_full' => new QueueFullException($status, $errorCode, $message),
            'unauthorized' => new UnauthorizedException($status, $errorCode, $message),
            default => new self($status, $errorCode, $message),
        };
    }
}
