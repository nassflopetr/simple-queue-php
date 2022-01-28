<?php

declare(strict_types = 1);

namespace NassFloPetr\SimpleQueue\Queues;

use NassFloPetr\SimpleQueue\Jobs\Job;

class RedisQueue implements Queue
{
    private \Redis $connection;

    private const QUEUE_KEY = 'queue';
    private const DELAYED_QUEUE_KEY = self::QUEUE_KEY . ':delayed';

    public function __construct(\Redis $connection)
    {
        if (!$connection->isConnected()) {
            throw new \Exception('Live redis connection are required.');
        }

        $this->connection = $connection;
    }

    public function pop(): ?Job
    {
        $this->migrateDelayedJobs();

        $job = $this->connection->lPop(self::QUEUE_KEY);

        return \is_string($job) ? \unserialize($job) : null;
    }

    public function push(Job $job): void
    {
        if ($job->isTimeToRun()) {
            if (!$this->connection->rPush(self::QUEUE_KEY, \serialize($job))) {
                throw new \Exception('Creation record error.');
            }
        } else {
            if (
                !$this->connection->zAdd(
                    self::DELAYED_QUEUE_KEY,
                    (string) $job->getExecuteAt()->getTimestamp(),
                    \serialize($job)
                )
            ) {
                throw new \Exception('Creation record error.');
            }
        }
    }

    private function migrateDelayedJobs(): void
    {
        $timestamp = (string) (new \DateTime())->getTimestamp();

        $jobs = $this->connection->zRangeByScore(self::DELAYED_QUEUE_KEY, '-inf', $timestamp);

        if (\count($jobs) > 0) {
            $this->connection->rPush(self::QUEUE_KEY, ...$jobs);

            $this->connection->zRemRangeByScore(self::DELAYED_QUEUE_KEY, '-inf', $timestamp);
        }
    }
}