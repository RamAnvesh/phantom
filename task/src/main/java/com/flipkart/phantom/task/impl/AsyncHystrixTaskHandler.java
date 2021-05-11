package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.task.spi.*;
import com.netflix.hystrix.HystrixCommandProperties;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class AsyncHystrixTaskHandler extends TaskHandler implements AsyncTaskHandler {

    /**

    /**
     * This method will be executed if execute() fails.
     * @param command the command used
     * @param params thrift parameters
     * @param data extra data if any
     * @return response
     */
    public abstract <T, S> CompletableFuture<TaskResult<T>> getFallBack(TaskContext taskContext, String command, Map<String, Object> params, S data);

    /**
     * Returns null. Sub-Classes should override it, if they need fallback functionality.
     * @param taskContext
     * @param command
     * @param taskRequestWrapper
     * @param decoder
     * @param <T>
     * @return
     * @throws RuntimeException
     */
    public <T, S> CompletableFuture<TaskResult<T>> getFallBack(TaskContext taskContext, String command, TaskRequestWrapper<S> taskRequestWrapper, Decoder<T> decoder) {
        return null;
    }

    /**
     * Call to optionally release any resources used to create the specified response. Useful when Hystrix command
     * timeouts result in the call being aborted. This callback is intended to be used for freeing up underlying connections/resources.
     * @param taskResult
     */
    public <T> void releaseResources(TaskResult<T> taskResult) {
        // do nothing
    }

    /**
     * Return the ExecutionIsolationStrategy. SEMAPHORE is the default.
     */
    public HystrixCommandProperties.ExecutionIsolationStrategy getIsolationStrategy() {
        return HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE;
    }

}
