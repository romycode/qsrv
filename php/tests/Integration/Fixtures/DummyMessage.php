<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Integration\Fixtures;

final class DummyMessage
{
    public function __construct(public readonly string $text)
    {
    }
}
