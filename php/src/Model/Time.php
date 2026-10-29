<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

/** @internal Conversions between PHP and the server's time formats. */
final class Time
{
    /**
     * Parses an RFC 3339 timestamp as sent by Go, nanoseconds included
     * (PHP keeps microseconds).
     */
    public static function parse(mixed $value): \DateTimeImmutable
    {
        if (!\is_string($value) || '' === $value) {
            return new \DateTimeImmutable('@0');
        }
        // 2026-10-29T19:33:00.123456789Z -> trim the fraction to 6 digits
        $value = preg_replace('/(\.\d{6})\d+/', '$1', $value) ?? $value;
        try {
            return new \DateTimeImmutable($value);
        } catch (\Exception) {
            throw new \UnexpectedValueException(\sprintf('qsrv: invalid timestamp "%s"', $value));
        }
    }

    /** Formats seconds as a Go duration string with millisecond precision. */
    public static function duration(float $seconds): string
    {
        if ($seconds < 0) {
            throw new \InvalidArgumentException('qsrv: duration must be >= 0');
        }

        return \sprintf('%dms', (int) round($seconds * 1000));
    }

    /** Parses a Go duration string ("1m30s", "500ms", "2h0m0s") into seconds. */
    public static function seconds(mixed $value): ?float
    {
        if (!\is_string($value) || '' === $value) {
            return null;
        }
        if ('0' === $value) {
            return 0.0;
        }
        $units = ['h' => 3600, 'm' => 60, 's' => 1, 'ms' => 0.001, 'us' => 1e-6, 'µs' => 1e-6, 'ns' => 1e-9];
        if (!preg_match_all('/(\d+(?:\.\d+)?)(h|ms|m|s|us|µs|ns)/u', $value, $m, \PREG_SET_ORDER)) {
            return null;
        }
        $total = 0.0;
        foreach ($m as [, $n, $unit]) {
            $total += (float) $n * $units[$unit];
        }

        return $total;
    }
}
