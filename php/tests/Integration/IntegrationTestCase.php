<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Integration;

use PHPUnit\Framework\TestCase;
use Romycode\Qsrv\Client;
use Romycode\Qsrv\Exception\QsrvException;

/**
 * Runs against a real qsrv. Start one and export:
 *
 *     QSRV_API_KEYS=$(openssl rand -hex 32) qsrv -data-dir /tmp/qsrv-test -addr 127.0.0.1:18080
 *     export QSRV_TEST_URL=http://127.0.0.1:18080 QSRV_TEST_API_KEY=<same key>
 */
abstract class IntegrationTestCase extends TestCase
{
    protected Client $client;
    /** @var list<string> */
    private array $queues = [];

    protected function setUp(): void
    {
        $url = getenv('QSRV_TEST_URL');
        $key = getenv('QSRV_TEST_API_KEY');
        if (!$url || !$key) {
            self::markTestSkipped('set QSRV_TEST_URL and QSRV_TEST_API_KEY to run integration tests');
        }
        $this->client = new Client($url, $key);
    }

    protected function tearDown(): void
    {
        // Delete DLQs last: a queue can't be deleted while it is another queue's DLQ.
        foreach (array_reverse($this->queues) as $q) {
            try {
                $this->client->deleteQueue($q);
            } catch (QsrvException) {
            }
        }
    }

    /** A unique queue name, deleted after the test. */
    protected function queueName(string $prefix = 'php'): string
    {
        return $this->queues[] = $prefix.'-'.bin2hex(random_bytes(6));
    }
}
