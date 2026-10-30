<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Integration\Fixtures;

use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;

/** Minimal PSR-11 container: Messenger's senders and retry strategy locators. */
final class ServiceMap implements ContainerInterface
{
    /** @param array<string, object> $services */
    public function __construct(private readonly array $services)
    {
    }

    public function get(string $id): object
    {
        return $this->services[$id] ?? throw new class("no service $id") extends \RuntimeException implements NotFoundExceptionInterface {};
    }

    public function has(string $id): bool
    {
        return isset($this->services[$id]);
    }
}
