package com.spotify.heroic.metric.memory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface IMultimap<K, V> {
    boolean put(K key, V value);

    Collection<V> get(K key);

    boolean remove(Object key, Object value);

    Collection<V> removeAll(Object key);

    Set<K> keySet();

    Collection<V> values();

    Set<Map.Entry<K, V>> entries();

    boolean containsKey(K key);

    boolean containsValue(Object value);

    boolean containsEntry(K key, V value);

    int size();

    void clear();
}
