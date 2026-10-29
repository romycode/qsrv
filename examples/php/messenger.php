<?php

/*
 * Symfony Messenger without the framework: route a message to a qsrv
 * transport, dispatch it (with an ordering group and a delay) and consume it
 * with a Worker. In a Symfony app the same is configuration: see
 * php/README.md ("Symfony Messenger").
 *
 *   composer install
 *   QSRV_ADDR=http://localhost:8080 QSRV_API_KEY=... php messenger.php
 */

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

use Example\SendInvoice;
use Psr\Container\ContainerInterface;
use Romycode\Qsrv\Client;
use Romycode\Qsrv\Exception\NotFoundException;
use Romycode\Qsrv\Messenger\QsrvGroupStamp;
use Romycode\Qsrv\Messenger\QsrvTransportFactory;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\SendMessageMiddleware;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Sender\SendersLocator;
use Symfony\Component\Messenger\Transport\Serialization\Serializer;
use Symfony\Component\Messenger\Worker;

$url = parse_url(getenv('QSRV_ADDR') ?: 'http://localhost:8080');
$dsn = sprintf(
    '%s://%s@%s:%d/example-php-invoices',
    'https' === ($url['scheme'] ?? 'http') ? 'qsrvs' : 'qsrv',
    rawurlencode((string) getenv('QSRV_API_KEY')),
    $url['host'],
    $url['port'] ?? 80,
);

// Start clean so the example is repeatable.
try {
    (new Client(getenv('QSRV_ADDR') ?: 'http://localhost:8080', (string) getenv('QSRV_API_KEY')))->deleteQueue('example-php-invoices');
} catch (NotFoundException) {
}

// The transport creates the queue on first use with these settings.
$transport = (new QsrvTransportFactory())->createTransport($dsn, [
    'wait_time' => 2,
    'queue_config' => ['visibility_timeout' => '30s', 'retry' => ['max_attempts' => 5]],
], Serializer::create()); // JSON body + "type" header: readable from Go too

// Routing: SendInvoice goes to the "qsrv" transport.
$senders = new SendersLocator([SendInvoice::class => ['qsrv']], new class($transport) implements ContainerInterface {
    public function __construct(private readonly object $transport)
    {
    }

    public function get(string $id): object
    {
        return $this->transport;
    }

    public function has(string $id): bool
    {
        return 'qsrv' === $id;
    }
});

$handled = 0;
$worker = null;
$handler = function (SendInvoice $m) use (&$handled, &$worker): void {
    printf("invoice for order %d (%s)\n", $m->orderId, $m->customer);
    if (3 === ++$handled) {
        $worker?->stop();
    }
};
$bus = new MessageBus([
    new SendMessageMiddleware($senders),
    new HandleMessageMiddleware(new HandlersLocator([SendInvoice::class => [$handler]])),
]);

// --- Dispatch: these go to qsrv, they are not handled here ------------------
$bus->dispatch(new SendInvoice(1, 'alice'), [new QsrvGroupStamp('alice')]); // ordered per customer
$bus->dispatch(new SendInvoice(2, 'alice'), [new QsrvGroupStamp('alice')]);
$bus->dispatch(new SendInvoice(3, 'bob'), [new DelayStamp(1500)]);         // in 1.5s
printf("dispatched 3, waiting in qsrv: %d\n", $transport->getMessageCount());

// --- Consume: what `bin/console messenger:consume` does ---------------------
$worker = new Worker(['qsrv' => $transport], $bus);
$worker->run(['sleep' => 100_000]);

printf("handled %d, left in the queue: %d\n", $handled, $transport->getMessageCount());
exit(3 === $handled ? 0 : 1);
