<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Romycode\Qsrv\Model\QueueConfig;
use Romycode\Qsrv\Model\RetryPolicy;

final class QueueConfigTest extends TestCase
{
    public function testDiffOnlyChecksWhatIsConfigured(): void
    {
        $server = QueueConfig::fromArray([
            'visibility_timeout' => '30s', 'ordering' => 'available', 'retention' => '336h0m0s',
            'retry' => ['max_attempts' => 5, 'initial_backoff' => '1s', 'max_backoff' => '1m0s', 'multiplier' => 2],
        ]);
        self::assertSame([], (new QueueConfig())->diff($server));
        self::assertSame([], (new QueueConfig(visibilityTimeout: 30, retry: new RetryPolicy()))->diff($server));

        $diff = (new QueueConfig(visibilityTimeout: 60, retry: RetryPolicy::none(), deadLetterQueue: 'dlq'))->diff($server);
        self::assertSame([
            'visibility_timeout' => ['60s', '30s'],
            'dead_letter_queue' => ['dlq', 'none'],
            'retry.max_attempts' => ['1', '5'],
        ], $diff);
        self::assertSame(['persistent' => ['true', 'false']], (new QueueConfig(persistent: true))->diff($server));
        self::assertTrue(QueueConfig::fromArray(['persistent' => 'true'])->persistent);
    }
}
