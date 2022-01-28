<?php

declare(strict_types=1);

namespace NassFloPetr\SimpleQueue\Jobs;

abstract class Job
{
    private bool $isCompleted;

    private int $numberOfFailedAttempts;

    private ?int $maxNumberOfFailedAttempts;

    private \DateTime $createdAt;

    private ?\DateTime $executeAt;

    private ?\DateInterval $executeAtIntervalAfterFailure;

    public function __construct(
        ?\DateTime $executeAt = null,
        ?int $maxNumberOfFailedAttempts = null,
        ?\DateInterval $executeAtIntervalAfterFailure = null,
    )
    {
        $this->isCompleted = false;

        $this->maxNumberOfFailedAttempts = $maxNumberOfFailedAttempts;

        $this->numberOfFailedAttempts = 0;

        $this->createdAt = new \DateTime();

        $this->executeAt = $executeAt;

        $this->executeAtIntervalAfterFailure = $executeAtIntervalAfterFailure;
    }

    public function __serialize(): array
    {
        return [
            'is_completed' => $this->isCompleted,
            'max_number_of_failed_attempts' => $this->maxNumberOfFailedAttempts,
            'number_of_failed_attempts' => $this->numberOfFailedAttempts,
            'created_at' => \serialize($this->createdAt),
            'execute_at' => !\is_null($this->executeAt)
                ? \serialize($this->executeAt)
                : null,
            'execute_at_interval_after_failure' => !\is_null($this->executeAtIntervalAfterFailure)
                ? \serialize($this->executeAtIntervalAfterFailure)
                : null,
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->isCompleted = $data['is_completed'];
        $this->maxNumberOfFailedAttempts = $data['max_number_of_failed_attempts'];
        $this->numberOfFailedAttempts = $data['number_of_failed_attempts'];
        $this->createdAt = \unserialize($data['created_at'], ['allowed_classes' => [\DateTime::class]]);
        $this->executeAt = !\is_null($data['execute_at'])
            ? \unserialize($data['execute_at'], ['allowed_classes' => [\DateTime::class]])
            : null;
        $this->executeAtIntervalAfterFailure = !\is_null($data['execute_at_interval_after_failure'])
            ? \unserialize($data['execute_at_interval_after_failure'], ['allowed_classes' => [\DateInterval::class]])
            : null;
    }

    abstract protected function handle(): void;

    public function run(): void
    {
        if (!$this->isTimeToRun()) {
            throw new \Exception(
                \sprintf('%s job must be executed no earlier than the specified time.', static::class)
            );
        }

        if ($this->isCompleted()) {
            throw new \Exception(\sprintf('%s job is completed already.', static::class));
        }

        if ($this->isFailed()) {
            throw new \Exception(\sprintf('%s job is failed.', static::class));
        }

        try {
            $this->handle();

            $this->setCompleted();
        } catch (\Exception $e) {
            $this->setFailedAttempt();

            throw $e;
        }
    }

    public function isCompleted(): bool
    {
        return $this->isCompleted;
    }

    public function isTimeToRun(): bool
    {
        return \is_null($this->executeAt) || (new \DateTime())->getTimestamp() >= $this->executeAt->getTimestamp();
    }

    public function isFailed(): bool
    {
        return !$this->isCompleted && $this->isNumberOfFailedAttemptsEnded();
    }

    public function getNumberOfFailedAttempts(): int
    {
        return $this->numberOfFailedAttempts;
    }

    public function getCreatedAt(): \DateTime
    {
        return $this->createdAt;
    }

    public function getExecuteAt(): ?\DateTime
    {
        return $this->executeAt;
    }

    public function setExecuteAt(?\DateTime $executeAt = null): void
    {
        $this->executeAt = $executeAt;
    }

    public function setExecuteAtIntervalAfterFailure(?\DateInterval $executeAtIntervalAfterFailure = null): void
    {
        $this->executeAtIntervalAfterFailure = $executeAtIntervalAfterFailure;
    }

    public function setMaxNumberOfFailedAttempts(?int $maxNumberOfFailedAttempts = null): void
    {
        $this->maxNumberOfFailedAttempts = $maxNumberOfFailedAttempts;
    }

    private function isNumberOfFailedAttemptsEnded(): bool
    {
        return !\is_null($this->maxNumberOfFailedAttempts)
            && $this->maxNumberOfFailedAttempts <= $this->numberOfFailedAttempts;
    }

    private function setCompleted(): void
    {
        $this->isCompleted = true;
    }

    private function setFailedAttempt(): void
    {
        ++$this->numberOfFailedAttempts;

        if (!\is_null($this->executeAtIntervalAfterFailure) && !$this->isNumberOfFailedAttemptsEnded()) {
            $this->setExecuteAt((new \DateTime())->add($this->executeAtIntervalAfterFailure));
        }
    }
}