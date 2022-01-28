<?php

declare(strict_types = 1);

namespace NassFloPetr\SimpleQueue\Queues;

use NassFloPetr\SimpleQueue\Jobs\Job;

interface Queue
{
    public function pop(): ?Job;

    public function push(Job $job): void;
}