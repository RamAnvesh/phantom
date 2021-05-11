package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.event.ServiceProxyEvent;
import com.flipkart.phantom.task.spi.*;
import com.flipkart.phantom.task.spi.interceptor.RequestInterceptor;
import com.flipkart.phantom.task.spi.interceptor.ResponseInterceptor;
import com.github.kristofa.brave.Brave;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import rx.Observable;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class AsyncTaskHandlerExecutor<S, R> extends HystrixObservableCommand<TaskResult<R>> implements Executor<TaskRequestWrapper<S>, TaskResult<R>> {

    /**
     * TaskResult message constants
     */
    public static final String NO_RESULT = "The command returned no result";

    /**
     * Event Type for publishing all events which are generated here
     */
    private static final String COMMAND_HANDLER = "COMMAND_HANDLER";

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
    private final List<RequestInterceptor<TaskRequestWrapper<S>>> requestInterceptors = new LinkedList<>();
    private final List<ResponseInterceptor<TaskResult<R>>> responseInterceptors = new LinkedList<>();

    private final AsyncHystrixTaskHandler taskHandler;

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
    protected AsyncTaskHandlerExecutor(AsyncHystrixTaskHandler taskHandler, TaskContext taskContext, String commandName,
                                       TaskRequestWrapper<S> taskRequestWrapper, int concurrentRequestSize) {
        this(taskHandler, taskContext, commandName, taskRequestWrapper, concurrentRequestSize, null);
    }

    protected AsyncTaskHandlerExecutor(AsyncHystrixTaskHandler taskHandler, TaskContext taskContext, String commandName,
            TaskRequestWrapper<S> taskRequestWrapper, int concurrentRequestSize, Decoder<R> decoder) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(taskHandler.getName()))
                .andCommandKey(HystrixCommandKey.Factory.asKey(commandName))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE).
                                withExecutionIsolationSemaphoreMaxConcurrentRequests(concurrentRequestSize)));
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
     * Interface method implementation. @see HystrixCommand#getFallback()
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Observable<TaskResult<R>> resumeWithFallback() {
        // check and populate execution error root cause, if any, for use in fallback
        if (this.isFailedExecution()) {
            this.params.put(Executor.EXECUTION_ERROR_CAUSE, this.getFailedExecutionException());
        }
        if (decoder == null) {
            return observableFrom(this.taskHandler.getFallBack(taskContext, command, params, data));
        } else {
            return observableFrom(this.taskHandler.getFallBack(taskContext, command, taskRequestWrapper, decoder));
        }
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
    public void addResponseInterceptor(ResponseInterceptor<TaskResult<R>> responseInterceptor) {
        this.responseInterceptors.add(responseInterceptor);
    }

    /**
     * Interface method implementation. Returns the name of the TaskHandler used by this Executor
     *
     * @return
     * @see com.flipkart.phantom.task.spi.Executor#getServiceName()
     */
    public com.google.common.base.Optional<String> getServiceName() {
        return com.google.common.base.Optional.of(this.taskHandler.getName());
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

    @Override
    public TaskResult<R> execute() {
        throw new UnsupportedOperationException("Doesn't support sync executor");
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

    void processResponseInterceptors(TaskResult<R> result, /*Nullable*/ RuntimeException exception) {
        for (ResponseInterceptor<TaskResult<R>> responseInterceptor : this.responseInterceptors) {
            responseInterceptor.process(result, com.google.common.base.Optional.fromNullable(exception));
        }
    }

    protected CompletableFuture<TaskResult<R>> getDefaultResult() {
        return completedFuture(new TaskResult<>(true, NO_RESULT));
    }

    @Override
    protected Observable<TaskResult<R>> construct() {
        return observableFrom(_run());
    }

    public CompletableFuture<TaskResult<R>> queue() {
        CompletableFuture<TaskResult<R>> completableFuture = new CompletableFuture<>();
        toObservable().subscribe(completableFuture::complete, completableFuture::completeExceptionally);
        return completableFuture;
    }

    private CompletableFuture<TaskResult<R>> _run() {
        this.eventBuilder.withRequestExecutionStartTime(System.currentTimeMillis());
        if (this.taskRequestWrapper.getRequestContext().isPresent() && this.taskRequestWrapper.getRequestContext().get().getCurrentServerSpan() != null) {
            Brave.getServerSpanThreadBinder().setCurrentSpan(this.taskRequestWrapper.getRequestContext().get().getCurrentServerSpan());
        }
        for (RequestInterceptor<TaskRequestWrapper<S>> requestInterceptor : this.requestInterceptors) {
            requestInterceptor.process(this.taskRequestWrapper);
        }
        CompletableFuture<TaskResult<R>> result;
        if (decoder == null) {
            result = this.taskHandler.executeAsync(taskContext, command, params, data);
        } else {
            result = this.taskHandler.executeAsync(taskContext, command, taskRequestWrapper, decoder);
        }
        if (result == null) {
            result = getDefaultResult();
        }
        return result.thenApply(taskResult -> {
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
                    taskResult,
                    Optional.of(throwable)
                            .map(RuntimeException::new)
                            .orElse(null));
        });
    }

    private Observable<TaskResult<R>> observableFrom(CompletableFuture<TaskResult<R>> resultFuture) {
        return Observable.create(subscriber ->
                resultFuture.whenComplete((result, error) -> {
                    if (error != null) {
                        subscriber.onError(error);
                    } else {
                        subscriber.onNext(result);
                        subscriber.onCompleted();
                    }
                }));
    }
}
