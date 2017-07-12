package com.spotify.heroic.metric.memory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocalMultimap<K, V> implements IMultimap<K, V> {
    private final Multimap<K, V> map = HashMultimap.create();

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
        return map.removeAll(key);
    }

    @Override
    public Set keySet() {
        return map.keySet();
    }

    @Override
    public Collection values() {
        return map.values();
    }

    @Override
    public Set<Map.Entry<K, V>> entries() {
        Set<Map.Entry<K, V>> set = new HashSet<>();
        set.addAll(map.entries());
        return set;
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
