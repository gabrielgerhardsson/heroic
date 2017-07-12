package com.spotify.heroic.metric.memory;

import com.hazelcast.core.MultiMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HazelcastMultimap<K, V> implements IMultimap<K, V> {
    private final MultiMap<K, V> map;

    @Override
    public boolean put(final K key, final V value) {
        return map.put(key, value);
    }

    @Override
    public Collection<V> get(final K key) {
        return map.get(key);
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return map.remove(key, value);
    }

    @Override
    public Collection<V> removeAll(final Object key) {
        return map.remove(key);
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Map.Entry<K, V>> entries() {
        return map.entrySet();
    }

    @Override
    public boolean containsKey(final K key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return map.containsValue(value);
    }

    @Override
    public boolean containsEntry(final K key, final V value) {
        return map.containsEntry(key, value);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void clear() {
        map.clear();
    }
}
