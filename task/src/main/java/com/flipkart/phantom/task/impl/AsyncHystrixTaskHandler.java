package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.task.spi.*;

import java.util.Map;

import static java.util.concurrent.CompletableFuture.completedFuture;

public abstract class AsyncHystrixTaskHandler extends HystrixTaskHandler implements AsyncTaskHandler {

    @Override
    public <T, S> AsyncTaskResult<T> executeAsync(TaskContext taskContext,
                                                  String command,
                                                  Map<String, Object> params,
                                                  S data) throws RuntimeException {
        return new AsyncTaskResult<>(completedFuture(this.execute(taskContext, command, params, data)));
    }

    @Override
    public <T, S> AsyncTaskResult<T> executeAsync(TaskContext taskContext,
                                                  String command,
                                                  TaskRequestWrapper<S> taskRequestWrapper,
                                                  Decoder<T> decoder) throws RuntimeException {
        return new AsyncTaskResult<>(completedFuture(execute(taskContext, command, taskRequestWrapper, decoder)));
    }
}
