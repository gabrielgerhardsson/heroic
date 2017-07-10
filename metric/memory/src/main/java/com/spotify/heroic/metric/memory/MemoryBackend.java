/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metric.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.hazelcast.core.MultiMap;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.sun.tools.javac.util.Pair;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.Data;
import lombok.ToString;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString(exclude = {"storage", "async", "createLock"})
public class MemoryBackend extends AbstractMetricBackend {
    public static final String MEMORY_KEYS = "memory-keys";

    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(MemoryBackend.class, "fetch");

    static final List<BackendEntry> EMPTY_ENTRIES = new ArrayList<>();

    static final Comparator<MemoryKey> COMPARATOR = (a, b) -> {
        final int t = a.getSource().compareTo(b.getSource());

        if (t != 0) {
            return t;
        }

        return a.getSeries().compareTo(b.getSeries());
    };

    private final Object createLock = new Object();

    private final AsyncFramework async;
    private final Groups groups;
    private final MultiMap<MemoryKey, Pair<Long, Metric>> storage;

    @Inject
    public MemoryBackend(
        final AsyncFramework async, final Groups groups,
        @Named("storage") final MultiMap<MemoryKey, Pair<Long, Metric>> storage,
        LifeCycleRegistry registry
    ) {
        super(async);
        this.async = async;
        this.groups = groups;
        this.storage = storage;
    }

    @Override
    public Statistics getStatistics() {
        return Statistics.of(MEMORY_KEYS, storage.size());
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public Groups groups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteMetric> write(WriteMetric.Request request) {
        final RequestTimer<WriteMetric> timer = WriteMetric.timer();
        writeOne(request);
        return async.resolved(timer.end());
    }

    @Override
    public AsyncFuture<FetchData> fetch(FetchData.Request request, FetchQuotaWatcher watcher) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(FETCH);
        final MemoryKey key = new MemoryKey(request.getType(), request.getSeries());
        final MetricCollection metrics = doFetch(key, request.getRange(), watcher);
        return async.resolved(FetchData.of(w.end(), ImmutableList.of(), ImmutableList.of(metrics)));
    }

    @Override
    public AsyncFuture<FetchData.Result> fetch(
        FetchData.Request request, FetchQuotaWatcher watcher,
        Consumer<MetricCollection> metricsConsumer
    ) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(FETCH);
        final MemoryKey key = new MemoryKey(request.getType(), request.getSeries());
        final MetricCollection metrics = doFetch(key, request.getRange(), watcher);
        metricsConsumer.accept(metrics);
        return async.resolved(FetchData.result(w.end()));
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return EMPTY_ENTRIES;
    }

    @Override
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
        storage.remove(new MemoryKey(key.getType(), key.getSeries()));
        return async.resolved();
    }

    @Data
    public static final class MemoryKey {
        private final MetricType source;
        private final Series series;
    }

    private AsyncFuture<WriteMetric> writeOne(final WriteMetric.Request request) {
        final MetricCollection g = request.getData();

        final MemoryKey key = new MemoryKey(g.getType(), request.getSeries());
        RequestTimer<WriteMetric> timer = WriteMetric.timer();
        return async.call(() -> {
            for (final Metric d : g.getData()) {
                storage.put(key, Pair.of(d.getTimestamp(), d));
            }
            return timer.end();
        });
    }

    private MetricCollection doFetch(
        final MemoryKey key, final DateRange range, final FetchQuotaWatcher watcher
    ) {
        final Collection<Pair<Long, Metric>> allValues = storage.get(key);

        if (allValues == null) {
            return MetricCollection.build(key.getSource(), ImmutableList.of());
        }

        
        final Iterable<Metric> metrics =
            tree.subMap(range.getStart(), false, range.getEnd(), true).values();
        final List<Metric> data = ImmutableList.copyOf(metrics);
        watcher.readData(data.size());
        return MetricCollection.build(key.getSource(), data);
    }

    /**
     * Get or create a new navigable map to store time data.
     *
     * @param key The key to create the map under.
     * @return An existing, or a newly created navigable map for the given key.
     */
    private NavigableMap<Long, Metric> putOrCreate(final MemoryKey key, final Consumer<NavigableMap<>) {
        final NavigableMap<Long, Metric> tree = storage.get(key);

        if (tree != null) {
            return tree;
        }

        synchronized (createLock) {
            final NavigableMap<Long, Metric> checked = storage.get(key);

            if (checked != null) {
                return checked;
            }

            final NavigableMap<Long, Metric> created = new TreeMap<>();
            storage.put(key, created);
            return created;
        }
    }
}
