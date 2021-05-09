package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.task.spi.*;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class AsyncTaskHandlerExecutor<S, R> extends AbstractTaskHandlerExecutor<S, AsyncTaskResult<R>, R> {

    private final AsyncHystrixTaskHandler taskHandler;

    public AsyncTaskHandlerExecutor(AsyncHystrixTaskHandler taskHandler,
                                    TaskContext taskContext,
                                    String refinedCommandName,
                                    int executorTimeOut,
                                    String refinedProxyName,
                                    int minConcurrencySize,
                                    int maxConcurrentSize,
                                    TaskRequestWrapper<S> requestWrapper,
                                    Decoder<R> decoder) {
        super(taskHandler, taskContext, refinedCommandName, executorTimeOut, refinedProxyName, minConcurrencySize, maxConcurrentSize, requestWrapper, decoder);
        this.taskHandler = taskHandler;
    }

    public AsyncTaskHandlerExecutor(AsyncHystrixTaskHandler taskHandler,
                                    TaskContext taskContext,
                                    String refinedCommandName,
                                    int executorTimeOut,
                                    String refinedProxyName,
                                    int minConcurrencySize,
                                    int maxConcurrentSize,
                                    TaskRequestWrapper requestWrapper) {
        super(taskHandler, taskContext, refinedCommandName, executorTimeOut, refinedProxyName, minConcurrencySize, maxConcurrentSize, requestWrapper);
        this.taskHandler = taskHandler;
    }

    public AsyncTaskHandlerExecutor(AsyncHystrixTaskHandler taskHandler,
                                    TaskContext taskContext,
                                    String refinedCommandName,
                                    TaskRequestWrapper requestWrapper,
                                    int maxConcurrentSize,
                                    Decoder<R> decoder) {
        super(taskHandler, taskContext, refinedCommandName, requestWrapper, maxConcurrentSize, decoder);
        this.taskHandler = taskHandler;
    }

    public AsyncTaskHandlerExecutor(AsyncHystrixTaskHandler taskHandler,
                                    TaskContext taskContext,
                                    String refinedCommandName,
                                    TaskRequestWrapper requestWrapper,
                                    int maxConcurrentSize) {
        super(taskHandler, taskContext, refinedCommandName, requestWrapper, maxConcurrentSize);
        this.taskHandler = taskHandler;
    }

    @Override
    protected AsyncTaskResult<R> getDefaultResult() {
        return new AsyncTaskResult<>(completedFuture(new TaskResult<>(true, AbstractTaskHandlerExecutor.NO_RESULT)));
    }

    @Override
    protected AsyncTaskResult<R> getResult(TaskContext taskContext, String command, Map<String, Object> params, S data) {
        return this.taskHandler.executeAsync(taskContext, command, params, data);
    }

    @Override
    protected AsyncTaskResult<R> getResult(TaskContext taskContext, String command, TaskRequestWrapper<S> requestWrapper, Decoder<R> decoder) {
        return this.taskHandler.executeAsync(taskContext, command, requestWrapper, decoder);
    }

    @Override
    protected AsyncTaskResult<R> run() {
        return new AsyncTaskResult<>(run().getTaskResultFuture()
                .thenApply(taskResult -> {
                    if (taskResult != null && !taskResult.isSuccess()) {
                        throw new RuntimeException("Command returned FALSE: " + taskResult.getMessage());
                    }
                    return taskResult;
                }).whenComplete((taskResult, throwable) -> {
                    if (taskResult != null
                            && this.isResponseTimedOut()) {
                        this.taskHandler.releaseResources(taskResult);
                    }
                    processResponseInterceptors(
                            new AsyncTaskResult<>(completedFuture(taskResult)),
                            Optional.of(throwable)
                                    .map(RuntimeException::new)
                                    .orElse(null));
                }));
    }

    @Override
    public CompletableFuture<AsyncTaskResult<R>> queue() {
        Future<AsyncTaskResult<R>> delegate = super.queue();
        CompletableFuture<AsyncTaskResult<R>> completableFuture = new CompletableFuture<AsyncTaskResult<R>>() {

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                delegate.cancel(mayInterruptIfRunning);
                return super.cancel(mayInterruptIfRunning);
            }
        };
        toObservable().subscribe(completableFuture::complete, completableFuture::completeExceptionally);
        return completableFuture;
    }

    public CompletableFuture<TaskResult<R>> queueAndCompose() {
        return queue().thenCompose(AsyncTaskResult::getTaskResultFuture);
    }
}
