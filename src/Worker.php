<?php

declare(strict_types = 1);

namespace NassFloPetr\SimpleQueue;

use Psr\Log\LoggerInterface;
use NassFloPetr\SimpleQueue\Queues\Queue;

class Worker
{
    private Queue $queue;
    private LoggerInterface $logger;

    public function __construct(Queue $queue, LoggerInterface $logger)
    {
        $this->queue = $queue;
        $this->logger = $logger;
    }

    public function __invoke()
    {
        try {
            while (true) {
                try {
                    $job = $this->queue->pop();

                    if (\is_null($job)) {
                        \sleep(3);

                        continue;
                    }

                    if (!$job->isTimeToRun()) {
                        $this->queue->push($job);

                        continue;
                    }

                    try {
                        $job->run();

                        $this->logger->info(
                            \sprintf(
                                '%s job was successfully completed. Number of failed attempts: %d.',
                                \get_class($job),
                                $job->getNumberOfFailedAttempts()
                            )
                        );
                    } catch (\Throwable $e) {
                        if (!$job->isCompleted() && !$job->isFailed()) {
                            $this->queue->push($job);
                        }

                        $this->logger->error($e->getMessage(), $e->getTrace());
                    }
                } catch (\Throwable $e) {
                    $this->logger->critical($e->getMessage(), $e->getTrace());
                }
            }
        } catch (\Throwable $e) {
            $this->logger->critical($e->getMessage(), $e->getTrace());
        }
    }
}
