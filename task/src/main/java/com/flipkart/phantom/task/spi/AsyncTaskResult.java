package com.flipkart.phantom.task.spi;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class AsyncTaskResult<T> implements AbstractTaskResult<T> {
    CompletableFuture<TaskResult<T>> taskResultFuture;

    public AsyncTaskResult(CompletableFuture<TaskResult<T>> taskResultFuture) {
        this.taskResultFuture = taskResultFuture;
    }

    public CompletableFuture<TaskResult<T>> getTaskResultFuture() {
        return taskResultFuture;
    }

    @Override
    public boolean isSuccess() {
        return taskResultFuture.isDone() &&
                !taskResultFuture.isCompletedExceptionally() &&
                Optional.ofNullable(taskResultFuture.getNow(null))
                        .map(TaskResult::isSuccess).orElse(false);
    }

}
