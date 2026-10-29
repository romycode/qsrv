<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Messenger;

/**
 * How a Messenger envelope is stored in a qsrv message body, so that other
 * languages can produce and consume the same queues:
 *
 *     {"body": "<serializer output>", "headers": {"type": "App\\Message\\Foo", ...}}
 *
 * "body_base64" replaces "body" when the serializer output is not UTF-8.
 * The headers are the Messenger serializer's (type, stamps, Content-Type).
 *
 * @internal
 */
final class WireFormat
{
    /** @param array{body: string, headers?: array<string, mixed>} $encoded */
    public static function encode(array $encoded): string
    {
        $body = (string) $encoded['body'];
        $out = 1 === preg_match('//u', $body) ? ['body' => $body] : ['body_base64' => base64_encode($body)];
        $out['headers'] = (object) ($encoded['headers'] ?? []);

        return json_encode($out, \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_SLASHES | \JSON_UNESCAPED_UNICODE);
    }

    /**
     * Bodies that are not this format (another producer sent raw payloads)
     * are passed to the serializer as-is with no headers.
     *
     * @return array{body: string, headers: array<string, mixed>}
     */
    public static function decode(string $raw): array
    {
        try {
            $data = json_decode($raw, true, 512, \JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return ['body' => $raw, 'headers' => []];
        }
        if (!\is_array($data) || !\array_key_exists('headers', $data) || !\is_array($data['headers'])
            || !(\is_string($data['body'] ?? null) || \is_string($data['body_base64'] ?? null))) {
            return ['body' => $raw, 'headers' => []];
        }
        if (isset($data['body_base64'])) {
            $body = base64_decode($data['body_base64'], true);
            if (false === $body) {
                return ['body' => $raw, 'headers' => []];
            }
        } else {
            $body = $data['body'];
        }

        return ['body' => $body, 'headers' => $data['headers']];
    }
}
