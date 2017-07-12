package com.spotify.heroic.metric.memory;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AsyncMultimap<K, V> {
    private final IMultimap<K, V> mm;
    private final AsyncFramework async;

    public AsyncFuture<Boolean> put(final K key, final V value) {
        return call(() -> mm.put(key, value));
    }

    public AsyncFuture<Collection<V>> get(final K key) {
        return call(() -> mm.get(key));
    }

    public AsyncFuture<Boolean> remove(final Object key, final Object value) {
        return call(() -> mm.remove(key, value));
    }

    public AsyncFuture<Collection<V>> removeAll(final Object key) {
        return call(() -> mm.removeAll(key));
    }

    public AsyncFuture<Set<K>> keySet() {
        return call(() -> mm.keySet());
    }

    public AsyncFuture<Collection<V>> values() {
        return call(() -> mm.values());
    }

    public AsyncFuture<Set<Map.Entry<K, V>>> entries() {
        return call(() -> mm.entries());
    }

    public AsyncFuture<Boolean> containsKey(final K key) {
        return call(() -> mm.containsKey(key));
    }

    public AsyncFuture<Boolean> containsValue(final Object value) {
        return call(() -> mm.containsValue(value));
    }

    public AsyncFuture<Boolean> containsEntry(final K key, final V value) {
        return call(() -> mm.containsEntry(key, value));
    }

    public AsyncFuture<Integer> size() {
        return call(() -> mm.size());
    }

    public AsyncFuture<Void> clear() {
        return call(() -> {
            mm.clear();
            return null;
        });
    }

    private <T> AsyncFuture<T> call(Callable<T> callable) {
        try {
            return async.call(callable);
        } catch (Exception e) {
            log.error("Async call failed for: " + callable.toString(), e);
            throw new RuntimeException(e);
        }
    }
}
