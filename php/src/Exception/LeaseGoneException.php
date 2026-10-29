<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Exception;

/** The lease expired or was already settled: the message may be delivered again. */
final class LeaseGoneException extends ApiException
{
}
