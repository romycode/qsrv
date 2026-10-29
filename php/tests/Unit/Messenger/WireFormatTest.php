<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Unit\Messenger;

use PHPUnit\Framework\TestCase;
use Romycode\Qsrv\Messenger\WireFormat;

final class WireFormatTest extends TestCase
{
    public function testTextRoundTrip(): void
    {
        $raw = WireFormat::encode(['body' => '{"id":"á/1"}', 'headers' => ['type' => 'App\Foo']]);
        self::assertSame('{"body":"{\"id\":\"á/1\"}","headers":{"type":"App\\\\Foo"}}', $raw);
        self::assertSame(['body' => '{"id":"á/1"}', 'headers' => ['type' => 'App\Foo']], WireFormat::decode($raw));
    }

    public function testBinaryBodyUsesBase64(): void
    {
        $raw = WireFormat::encode(['body' => "a\xffb"]);
        self::assertSame(['body_base64' => base64_encode("a\xffb"), 'headers' => []], json_decode($raw, true));
        self::assertSame(['body' => "a\xffb", 'headers' => []], WireFormat::decode($raw));
    }

    public function testEmptyHeadersEncodeAsObject(): void
    {
        self::assertStringContainsString('"headers":{}', WireFormat::encode(['body' => 'x', 'headers' => []]));
    }

    public function testForeignBodiesPassThrough(): void
    {
        foreach (['plain text', '{"order":1}', '[1,2]', '{"body":"x"}', '{"body":1,"headers":{}}'] as $foreign) {
            self::assertSame(['body' => $foreign, 'headers' => []], WireFormat::decode($foreign), $foreign);
        }
    }
}
