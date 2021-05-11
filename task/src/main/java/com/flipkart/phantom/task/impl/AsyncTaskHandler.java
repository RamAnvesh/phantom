package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.task.spi.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface AsyncTaskHandler{
    /**
     * Execute this task in a non-blocking fashion if possible: ideally no threads should be blocked by the returned CompletableFuture.
     * Implementers who can execute the task in such a way (by using non-blocking I/O feature of the OS, for example)
     * should extend this class and implement this method.
     *
     * @param taskContext
     * @param command
     * @param params
     * @param data
     * @param <T>
     * @param <S>
     * @return
     * @throws RuntimeException
     */
    <T, S> CompletableFuture<TaskResult<T>> executeAsync(TaskContext taskContext,
                                           String command,
                                           Map<String, Object> params,
                                           S data);

    /**
     * Execute this task in a non-blocking fashion if possible: ideally no threads should be blocked by the returned CompletableFuture.
     *
     * @param taskContext
     * @param command
     * @param taskRequestWrapper
     * @param decoder
     * @param <T>
     * @param <S>
     * @return
     * @throws RuntimeException
     */
    <T, S> CompletableFuture<TaskResult<T>> executeAsync(TaskContext taskContext,
                                           String command,
                                           TaskRequestWrapper<S> taskRequestWrapper,
                                           Decoder<T> decoder);
}
