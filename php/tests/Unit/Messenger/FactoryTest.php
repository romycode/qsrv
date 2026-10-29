<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Unit\Messenger;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Romycode\Qsrv\Messenger\QsrvTransport;
use Romycode\Qsrv\Messenger\QsrvTransportFactory;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

final class FactoryTest extends TestCase
{
    public function testSupports(): void
    {
        $f = new QsrvTransportFactory();
        self::assertTrue($f->supports('qsrv://k@h/q', []));
        self::assertTrue($f->supports('qsrvs://k@h/q', []));
        self::assertFalse($f->supports('redis://h', []));
    }

    public function testParseFullDsn(): void
    {
        $c = QsrvTransportFactory::parse('qsrv://my%2Bkey@qsrv.internal:8080/orders%20v2?wait_time=20&auto_setup=false&on_reject=nack&timeout=3');
        self::assertSame('http://qsrv.internal:8080', $c['url']);
        self::assertSame('my+key', $c['api_key']);
        self::assertSame('orders v2', $c['queue']);
        self::assertSame(20.0, $c['wait_time']);
        self::assertFalse($c['auto_setup']);
        self::assertSame('nack', $c['on_reject']);
        self::assertSame(3.0, $c['timeout']);
    }

    public function testHttpsDefaultsAndOptions(): void
    {
        $c = QsrvTransportFactory::parse('qsrvs://qsrv.example.com', [
            'api_key' => 'k', 'queue' => 'q',
            'queue_config' => ['visibility_timeout' => '1m', 'retry' => ['max_attempts' => 3, 'initial_backoff' => 2], 'retention' => 0],
        ]);
        self::assertSame('https://qsrv.example.com', $c['url']);
        self::assertSame(5.0, $c['wait_time']);
        self::assertTrue($c['auto_setup']);
        self::assertSame('ack', $c['on_reject']);
        self::assertSame(60.0, $c['queue_config']->visibilityTimeout);
        self::assertSame(3, $c['queue_config']->retry?->maxAttempts);
        self::assertSame(2.0, $c['queue_config']->retry?->initialBackoff);
        self::assertSame(0.0, $c['queue_config']->retention);
    }

    public function testOptionsOverrideDsn(): void
    {
        $c = QsrvTransportFactory::parse('qsrv://dsnkey@h/dsnqueue?wait_time=1', ['api_key' => 'optkey', 'queue' => 'optqueue', 'wait_time' => 2]);
        self::assertSame(['optkey', 'optqueue', 2.0], [$c['api_key'], $c['queue'], $c['wait_time']]);
    }

    public function testApiKeyAsPassword(): void
    {
        self::assertSame('secret', QsrvTransportFactory::parse('qsrv://:secret@h/q')['api_key']);
    }

    /** @return iterable<string, array{string, array<string, mixed>}> */
    public static function invalid(): iterable
    {
        yield 'no key' => ['qsrv://h/q', []];
        yield 'no queue' => ['qsrv://k@h', []];
        yield 'unknown option' => ['qsrv://k@h/q?wiat_time=5', []];
        yield 'wait too long' => ['qsrv://k@h/q?wait_time=21', []];
        yield 'bad bool' => ['qsrv://k@h/q?auto_setup=maybe', []];
        yield 'garbage' => ['qsrv://', []];
    }

    #[DataProvider('invalid')]
    public function testInvalid(string $dsn, array $options): void
    {
        $this->expectException(InvalidArgumentException::class);
        QsrvTransportFactory::parse($dsn, $options);
    }

    public function testCreateTransportIgnoresTransportName(): void
    {
        $t = (new QsrvTransportFactory())->createTransport('qsrv://k@h/q', ['transport_name' => 'async'], new PhpSerializer());
        self::assertInstanceOf(QsrvTransport::class, $t);
        self::assertSame('q', $t->getQueueName());
    }

    public function testInvalidOnReject(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        (new QsrvTransportFactory())->createTransport('qsrv://k@h/q?on_reject=drop', [], new PhpSerializer());
    }
}
