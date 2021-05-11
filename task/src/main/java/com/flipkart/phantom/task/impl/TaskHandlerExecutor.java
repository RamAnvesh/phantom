package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.task.spi.Decoder;
import com.flipkart.phantom.task.spi.TaskContext;
import com.flipkart.phantom.task.spi.TaskRequestWrapper;
import com.flipkart.phantom.task.spi.TaskResult;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;

public class TaskHandlerExecutor<S, R> extends AbstractTaskHandlerExecutor<S, TaskResult<R>, R> {
    protected TaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, int timeout, String threadPoolName, int coreThreadPoolSize, int maxThreadPoolSize, TaskRequestWrapper<S> taskRequestWrapper) {
        super(taskHandler, taskContext, commandName, timeout, threadPoolName, coreThreadPoolSize, maxThreadPoolSize, taskRequestWrapper);
    }

    protected TaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, int timeout, String threadPoolName, int coreThreadPoolSize, int maxThreadPoolSize, TaskRequestWrapper<S> taskRequestWrapper, Decoder<R> decoder) {
        super(taskHandler, taskContext, commandName, timeout, threadPoolName, coreThreadPoolSize, maxThreadPoolSize, taskRequestWrapper, decoder);
    }

    protected TaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, TaskRequestWrapper<S> taskRequestWrapper, int concurrentRequestSize) {
        super(taskHandler, taskContext, commandName, taskRequestWrapper, concurrentRequestSize);
    }

    protected TaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, TaskRequestWrapper<S> taskRequestWrapper, int concurrentRequestSize, Decoder<R> decoder) {
        super(taskHandler, taskContext, commandName, taskRequestWrapper, concurrentRequestSize, decoder);
    }

    public TaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, int executorTimeout, TaskRequestWrapper<S> taskRequestWrapper) {
        super(taskHandler, taskContext, commandName, executorTimeout, taskRequestWrapper);
    }

    public TaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, int executorTimeout, TaskRequestWrapper<S> taskRequestWrapper, Decoder<R> decoder) {
        super(taskHandler, taskContext, commandName, executorTimeout, taskRequestWrapper, decoder);
    }

    @Override
    protected TaskResult<R> getDefaultResult() {
        return new TaskResult<>(true, AbstractTaskHandlerExecutor.NO_RESULT);
    }

    @Override
    protected TaskResult<R> getResult(TaskContext taskContext, String command, TaskRequestWrapper<S> requestWrapper, Decoder<R> decoder) {
        return this.taskHandler.execute(taskContext, command, requestWrapper, decoder);
    }

    @Override
    protected TaskResult<R> getResult(TaskContext taskContext, String command, Map<String, Object> params, S data) {
        return this.taskHandler.execute(taskContext, command, params, data);
    }

    @Override
    protected TaskResult<R> run() throws Exception {
        Optional<RuntimeException> transportException = Optional.empty();
        TaskResult<R> result = null;
        try {
            result = _run();
        } catch (RuntimeException e) {
            transportException = Optional.of(e);
            throw e; // rethrow this for it to handled by other layers in the call stack
        } finally {
            // signal to the handler to release resources if the command timed out
            if (this.isResponseTimedOut()) {
                if (result != null) {
                    if (HystrixTaskHandler.class.isAssignableFrom(this.taskHandler.getClass())) {
                        ((HystrixTaskHandler) this.taskHandler).releaseResources((TaskResult<? extends Object>) result);
                    }
                }
            }
            processResponseInterceptors(result, transportException.orElse(null));
        }
        if (result != null && !result.isSuccess()) {
            throw new RuntimeException("Command returned FALSE: " + result.getMessage());
        }
        return result;
    }

    @Override
    public Future<TaskResult<R>> queue() {
        return super.queue();
    }
}
