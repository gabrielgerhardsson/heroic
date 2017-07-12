package com.spotify.heroic.metric.memory;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import eu.toolchain.async.AsyncFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class LocalICompleteableFuture<T> implements ICompletableFuture<T> {
    private final AsyncFuture<T> future;

    @Override
    public void andThen(final ExecutionCallback<T> callback) {
        future.onResolved(callback::onResponse);
        future.onFailed(callback::onFailure);
    }

    @Override
    public void andThen(final ExecutionCallback<T> callback, final Executor executor) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return future.cancel();
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public T get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }
}
