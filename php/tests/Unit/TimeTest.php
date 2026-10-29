<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Romycode\Qsrv\Model\Time;

final class TimeTest extends TestCase
{
    /** @return iterable<array{string, float}> */
    public static function goDurations(): iterable
    {
        yield ['30s', 30.0];
        yield ['1m30s', 90.0];
        yield ['2160h0m0s', 7776000.0];
        yield ['500ms', 0.5];
        yield ['1.5s', 1.5];
        yield ['0s', 0.0];
        yield ['0', 0.0];
    }

    #[DataProvider('goDurations')]
    public function testParsesGoDurations(string $in, float $seconds): void
    {
        self::assertEqualsWithDelta($seconds, Time::seconds($in), 1e-9);
    }

    public function testFormatsMilliseconds(): void
    {
        self::assertSame('1500ms', Time::duration(1.5));
        self::assertSame('0ms', Time::duration(0));
    }

    public function testParsesNanosecondTimestamps(): void
    {
        self::assertSame('2026-10-29 19:33:00.123456', Time::parse('2026-10-29T19:33:00.123456789Z')->format('Y-m-d H:i:s.u'));
        self::assertSame('2026-10-29T21:33:00+02:00', Time::parse('2026-10-29T21:33:00+02:00')->format(\DATE_ATOM));
    }
}
