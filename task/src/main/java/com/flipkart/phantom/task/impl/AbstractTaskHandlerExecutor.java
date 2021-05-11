/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.event.ServiceProxyEvent;
import com.flipkart.phantom.task.spi.*;
import com.flipkart.phantom.task.spi.interceptor.RequestInterceptor;
import com.flipkart.phantom.task.spi.interceptor.ResponseInterceptor;
import com.github.kristofa.brave.Brave;
import com.google.common.base.Optional;
import com.netflix.hystrix.*;
import com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * <code>TaskHandlerExecutor</code> is an extension of {@link HystrixCommand}. It is essentially a
 * wrapper around {@link TaskHandler}, providing a means for the TaskHandler to to be called using
 * a Hystrix Command.
 *
 * @author devashishshankar
 * @version 1.0, 19th March, 2013
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractTaskHandlerExecutor<S, T extends AbstractTaskResult, R> extends HystrixCommand<T> implements Executor<TaskRequestWrapper<S>, T> {

    /**
     * TaskResult message constants
     */
    public static final String NO_RESULT = "The command returned no result";
    public static final String ASYNC_QUEUED = "The command dispatched for async execution";

    /**
     * The default Hystrix group to which the command belongs, unless otherwise mentioned
     */
    public static final String DEFAULT_HYSTRIX_GROUP = "DEFAULT_GROUP";

    /**
     * The default Hystrix Thread pool to which this command belongs, unless otherwise mentioned
     */
    public static final String DEFAULT_HYSTRIX_THREAD_POOL = "DEFAULT_THREAD_POOL";

    /**
     * The default Hystrix Thread pool to which this command belongs, unless otherwise mentioned
     */
    public static final int DEFAULT_HYSTRIX_THREAD_POOL_SIZE = 10;

    /**
     * Event Type for publishing all events which are generated here
     */
    private static final String COMMAND_HANDLER = "COMMAND_HANDLER";

    /**
     * The {@link TaskHandler} or {@link HystrixTaskHandler} instance which this Command wraps around
     */
    protected TaskHandler taskHandler;

    /**
     * The params required to execute a TaskHandler
     */
    protected TaskContext taskContext;

    /**
     * The command for which this task is executed
     */
    protected String command;

    /**
     * The parameters which are utilized by the task for execution
     */
    protected Map<String, Object> params;

    /**
     * Data which is utilized by the task for execution
     */
    protected S data;

    /* Task Request Wrapper */
    protected TaskRequestWrapper<S> taskRequestWrapper;

    /* Decoder to decode requests */
    protected Decoder<R> decoder;

    /**
     * Event which records various paramenters of this request execution & published later
     */
    protected ServiceProxyEvent.Builder eventBuilder;

    /**
     * List of request and response interceptors
     */
    private List<RequestInterceptor<TaskRequestWrapper<S>>> requestInterceptors = new LinkedList<RequestInterceptor<TaskRequestWrapper<S>>>();
    private List<ResponseInterceptor<T>> responseInterceptors = new LinkedList<>();

    /**
     * Basic constructor for {@link TaskHandler}. The Hystrix command name is commandName. The group name is the Handler Name
     * (HystrixTaskHandler#getName)
     *
     * @param taskHandler        The taskHandler to be wrapped
     * @param taskContext        The context (Unique context required by Handlers to communicate with the container.)
     * @param commandName        name of the command
     * @param timeout            the timeout for the Hystrix thread
     * @param threadPoolName     Name of the thread pool
     * @param coreThreadPoolSize core size of the thread pool
     * @param maxThreadPoolSize  max size of the thread pool
     * @param taskRequestWrapper requestWrapper containing the data and the parameters
     */
    protected AbstractTaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, int timeout,
                                          String threadPoolName, int coreThreadPoolSize, int maxThreadPoolSize, TaskRequestWrapper<S> taskRequestWrapper) {
        this(taskHandler, taskContext, commandName, timeout, threadPoolName, coreThreadPoolSize, maxThreadPoolSize, taskRequestWrapper, null);
    }

    /**
     * Basic constructor for {@link TaskHandler}. The Hystrix command name is commandName. The group name is the Handler Name
     * (HystrixTaskHandler#getName)
     *
     * @param taskHandler        The taskHandler to be wrapped
     * @param taskContext        The context (Unique context required by Handlers to communicate with the container.)
     * @param commandName        name of the command
     * @param timeout            the timeout for the Hystrix thread
     * @param threadPoolName     Name of the thread pool
     * @param coreThreadPoolSize core size of the thread pool
     * @param maxThreadPoolSize  max size of the thread pool
     * @param taskRequestWrapper requestWrapper containing the data and the parameters
     * @param decoder            Decoder sent by the Client
     */
    protected AbstractTaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, int timeout,
                                          String threadPoolName, int coreThreadPoolSize, int maxThreadPoolSize, TaskRequestWrapper<S> taskRequestWrapper, Decoder<R> decoder) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(taskHandler.getName()))
                .andCommandKey(HystrixCommandKey.Factory.asKey(commandName))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(taskHandler.getVersionedThreadPoolName(threadPoolName)))
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withAllowMaximumSizeToDivergeFromCoreSize(true).withCoreSize(coreThreadPoolSize >= 0 ? coreThreadPoolSize : maxThreadPoolSize).withMaximumSize(maxThreadPoolSize))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(timeout)));
        this.taskHandler = taskHandler;
        this.taskContext = taskContext;
        this.command = commandName;
        this.data = taskRequestWrapper.getData();
        this.params = taskRequestWrapper.getParams();
        this.taskRequestWrapper = taskRequestWrapper;
        this.eventBuilder = new ServiceProxyEvent.Builder(commandName, COMMAND_HANDLER);
        this.decoder = decoder;
    }

    /**
     * Constructor for {@link TaskHandler} using Semaphore isolation. The Hystrix command name is commandName. The group name is the Handler Name
     * (HystrixTaskHandler#getName)
     *
     * @param taskHandler           The taskHandler to be wrapped
     * @param taskContext           The context (Unique context required by Handlers to communicate with the container.)
     * @param commandName           name of the command
     * @param taskRequestWrapper    requestWrapper containing the data and the parameters
     * @param concurrentRequestSize no of Max Concurrent requests which can be served
     */
    protected AbstractTaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName,
                                          TaskRequestWrapper<S> taskRequestWrapper, int concurrentRequestSize) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(taskHandler.getName()))
                .andCommandKey(HystrixCommandKey.Factory.asKey(commandName))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(ExecutionIsolationStrategy.SEMAPHORE).
                                withExecutionIsolationSemaphoreMaxConcurrentRequests(concurrentRequestSize)))
        ;
        this.taskHandler = taskHandler;
        this.taskContext = taskContext;
        this.command = commandName;
        this.data = taskRequestWrapper.getData();
        this.params = taskRequestWrapper.getParams();
        this.taskRequestWrapper = taskRequestWrapper;
        this.eventBuilder = new ServiceProxyEvent.Builder(commandName, COMMAND_HANDLER);
    }

    /**
     * Constructor for {@link TaskHandler} using Semaphore isolation. The Hystrix command name is commandName. The group name is the Handler Name
     * (HystrixTaskHandler#getName)
     *
     * @param taskHandler           The taskHandler to be wrapped
     * @param taskContext           The context (Unique context required by Handlers to communicate with the container.)
     * @param commandName           name of the command
     * @param taskRequestWrapper    requestWrapper containing the data and the parameters
     * @param decoder               Decoder sent by the Client
     * @param concurrentRequestSize no of Max Concurrent requests which can be served
     */
    protected AbstractTaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName,
                                          TaskRequestWrapper<S> taskRequestWrapper, int concurrentRequestSize, Decoder decoder) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(taskHandler.getName()))
                .andCommandKey(HystrixCommandKey.Factory.asKey(commandName))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(ExecutionIsolationStrategy.SEMAPHORE).
                                withExecutionIsolationSemaphoreMaxConcurrentRequests(concurrentRequestSize)))
        ;
        this.taskHandler = taskHandler;
        this.taskContext = taskContext;
        this.command = commandName;
        this.data = taskRequestWrapper.getData();
        this.params = taskRequestWrapper.getParams();
        this.taskRequestWrapper = taskRequestWrapper;
        this.decoder = decoder;
        this.eventBuilder = new ServiceProxyEvent.Builder(commandName, COMMAND_HANDLER);
    }

    /**
     * Constructor for TaskHandlerExecutor run through Default Hystrix Thread Pool ({@link AbstractTaskHandlerExecutor#DEFAULT_HYSTRIX_THREAD_POOL})
     *
     * @param taskHandler        The taskHandler to be wrapped
     * @param taskContext        The context (Unique context required by Handlers to communicate with the container.)
     * @param commandName        name of the command
     * @param executorTimeout    the timeout for the Hystrix thread
     * @param taskRequestWrapper requestWrapper containing the data and the parameters
     */
    public AbstractTaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, int executorTimeout,
                                       TaskRequestWrapper<S> taskRequestWrapper) {
        this(taskHandler, taskContext, commandName, executorTimeout, DEFAULT_HYSTRIX_THREAD_POOL, DEFAULT_HYSTRIX_THREAD_POOL_SIZE, DEFAULT_HYSTRIX_THREAD_POOL_SIZE, taskRequestWrapper);
    }

    /**
     * Constructor for TaskHandlerExecutor run through Default Hystrix Thread Pool ({@link AbstractTaskHandlerExecutor#DEFAULT_HYSTRIX_THREAD_POOL})
     *
     * @param taskHandler        The taskHandler to be wrapped
     * @param taskContext        The context (Unique context required by Handlers to communicate with the container.)
     * @param commandName        name of the command
     * @param executorTimeout    the timeout for the Hystrix thread
     * @param decoder            Decoder sent by the Client
     * @param taskRequestWrapper requestWrapper containing the data and the parameters
     */
    public AbstractTaskHandlerExecutor(TaskHandler taskHandler, TaskContext taskContext, String commandName, int executorTimeout,
                                       TaskRequestWrapper<S> taskRequestWrapper, Decoder decoder) {
        this(taskHandler, taskContext, commandName, executorTimeout, DEFAULT_HYSTRIX_THREAD_POOL, DEFAULT_HYSTRIX_THREAD_POOL_SIZE,
                DEFAULT_HYSTRIX_THREAD_POOL_SIZE, taskRequestWrapper, decoder);
    }

    /**
     * Common {@link #run()} code which can be used by both the sync ({@link TaskHandlerExecutor})
     * and async ({@link AsyncTaskHandlerExecutor} implementations.
     */
    protected T _run() {
        this.eventBuilder.withRequestExecutionStartTime(System.currentTimeMillis());
        if (this.taskRequestWrapper.getRequestContext().isPresent() && this.taskRequestWrapper.getRequestContext().get().getCurrentServerSpan() != null) {
            Brave.getServerSpanThreadBinder().setCurrentSpan(this.taskRequestWrapper.getRequestContext().get().getCurrentServerSpan());
        }
        for (RequestInterceptor<TaskRequestWrapper<S>> requestInterceptor : this.requestInterceptors) {
            requestInterceptor.process(this.taskRequestWrapper);
        }
        T result;
        if (decoder == null) {
            result = getResult(taskContext, command, params, data);
        } else {
            result = getResult(taskContext, command, taskRequestWrapper, decoder);
        }
        if (result == null) {
            result = getDefaultResult();
        }
        return result;
    }

    protected abstract T getDefaultResult();

    /**
     * Runs the critical section of the result computation. Abstract so that implementations can choose whether
     * to go with a sync implementation or an async one.
     */
    protected abstract T getResult(TaskContext taskContext, String command, Map<String, Object> params, S data);

    /**
     * Runs the critical section of the result computation. Abstract so that implementations can choose whether
     * to go with a sync implementation or an async one.
     */
    protected abstract T getResult(TaskContext taskContext, String command, TaskRequestWrapper<S> params, Decoder<R> decoder);

    /**
     * Interface method implementation. @see HystrixCommand#getFallback()
     */
    @SuppressWarnings("unchecked")
    @Override
    protected T getFallback() {
        // check and populate execution error root cause, if any, for use in fallback
        if (this.isFailedExecution()) {
            this.params.put(Executor.EXECUTION_ERROR_CAUSE, this.getFailedExecutionException());
        }
        if (this.taskHandler instanceof HystrixTaskHandler) {
            HystrixTaskHandler hystrixTaskHandler = (HystrixTaskHandler) this.taskHandler;
            if (decoder == null) {
                return (T) hystrixTaskHandler.getFallBack(taskContext, command, params, data);
            } else {
                return (T) hystrixTaskHandler.getFallBack(taskContext, command, taskRequestWrapper, decoder);
            }
        }
        return null;
    }

    /**
     * Interface method implementation. Adds the RequestInterceptor to the list of request interceptors that will be invoked
     *
     * @see com.flipkart.phantom.task.spi.Executor#addRequestInterceptor(com.flipkart.phantom.task.spi.interceptor.RequestInterceptor)
     */
    public void addRequestInterceptor(RequestInterceptor<TaskRequestWrapper<S>> requestInterceptor) {
        this.requestInterceptors.add(requestInterceptor);
    }

    /**
     * Interface method implementation. Adds the ResponseInterceptor to the list of response interceptors that will be invoked
     *
     * @see com.flipkart.phantom.task.spi.Executor#addResponseInterceptor(com.flipkart.phantom.task.spi.interceptor.ResponseInterceptor)
     */
    public void addResponseInterceptor(ResponseInterceptor<T> responseInterceptor) {
        this.responseInterceptors.add(responseInterceptor);
    }

    /**
     * Interface method implementation. Returns the name of the TaskHandler used by this Executor
     *
     * @see com.flipkart.phantom.task.spi.Executor#getServiceName()
     */
    public Optional<String> getServiceName() {
        return Optional.of(this.taskHandler.getName());
    }

    /**
     * Interface method implementation. Returns the TaskRequestWrapper instance that this Executor was created with
     *
     * @see com.flipkart.phantom.task.spi.Executor#getRequestWrapper()
     */
    public TaskRequestWrapper<S> getRequestWrapper() {
        return this.taskRequestWrapper;
    }

    /**
     * First It checks the call invocation type has been overridden for the command,
     * if not, it defaults to task handler call invocation type.
     *
     * @return callInvocation Type
     */
    public int getCallInvocationType() {
        if (this.taskHandler.getCallInvocationTypePerCommand() != null) {
            Integer callInvocationType = this.taskHandler.getCallInvocationTypePerCommand().get(this.command);
            if (callInvocationType != null) {
                return callInvocationType;
            }
        }
        return this.taskHandler.getCallInvocationType();
    }

    /**
     * Getter method for the event builder
     */
    public ServiceProxyEvent.Builder getEventBuilder() {
        return eventBuilder;
    }

    void processRequestInterceptors() {
        for (RequestInterceptor<TaskRequestWrapper<S>> responseInterceptor : this.requestInterceptors) {
            responseInterceptor.process(this.taskRequestWrapper);
        }
    }

    void processResponseInterceptors(T result, /*Nullable*/ RuntimeException exception) {
        for (ResponseInterceptor<T> responseInterceptor : this.responseInterceptors) {
            responseInterceptor.process(result, Optional.fromNullable(exception));
        }
    }

}
