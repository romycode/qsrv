<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Exception;

/** The server could not be reached or the response could not be read. */
final class TransportException extends \RuntimeException implements QsrvException
{
}
