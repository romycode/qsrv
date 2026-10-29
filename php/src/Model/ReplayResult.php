<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Model;

final readonly class ReplayResult
{
    public function __construct(
        public int $replayed,
        /** Pass as $cursor to get the next page; null = done. */
        public ?int $nextCursor,
    ) {
    }
}
