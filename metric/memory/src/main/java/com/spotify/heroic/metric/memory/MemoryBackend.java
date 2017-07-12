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
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.spotify.heroic.ObjectHasher;
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
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.StreamCollector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@Slf4j
@ToString(exclude = {"storage", "async", "createLock"})
public class MemoryBackend extends AbstractMetricBackend {
    public static final String MEMORY_KEYS = "memory-keys";

    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(MemoryBackend.class, "fetch");

    // 12 hours
    private static final long CHUNK_SIZE_MS = 12 * 3600 * 1000;

    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();
    private static final int WRITE_PARALLELISM = 100;
    private static final int READ_PARALLELISM = 100;

    static final List<BackendEntry> EMPTY_ENTRIES = new ArrayList<>();

    static final Comparator<MemoryKey> COMPARATOR = (a, b) -> {
        final int t = a.getSource().compareTo(b.getSource());

        if (t != 0) {
            return t;
        }

        return a.getSeries().compareTo(b.getSeries());
    };

    private final AsyncFramework async;
    private final Groups groups;
    private final HazelcastInstance instance;

    @Inject
    public MemoryBackend(
        final AsyncFramework async, final Groups groups,
        @Named("servers") final List<String> servers, LifeCycleRegistry registry
    ) {
        super(async);
        this.async = async;
        this.groups = groups;
        this.instance = Hazelcast.getClientInstance(servers);
    }

    @Override
    public Statistics getStatistics() {
        /*
        try {
            return Statistics.of(MEMORY_KEYS, metrics.size().get());
        } catch (Exception e) {
            log.error("Async call failed: " + e);
            throw new RuntimeException(e);
        }
        */
        throw new RuntimeException("Not implemented");
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
        return writeOne(request).onFailed(e -> log.error("Failed to write: ", e));
    }

    @Override
    public AsyncFuture<FetchData> fetch(FetchData.Request request, FetchQuotaWatcher watcher) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(FETCH);
        final MemoryKey key = new MemoryKey(request.getType(), request.getSeries());
        final AsyncFuture<MetricCollection> metrics = doFetch(key, request.getRange(), watcher);
        return metrics
            .directTransform(mc -> FetchData.of(w.end(), ImmutableList.of(), ImmutableList.of(mc)))
            .directTransform(result -> {
                log.info("Read finished in " + result.getResult().getTrace().elapsed());
                return result;
            });
    }

    @Override
    public AsyncFuture<FetchData.Result> fetch(
        FetchData.Request request, FetchQuotaWatcher watcher,
        Consumer<MetricCollection> metricsConsumer
    ) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(FETCH);
        final MemoryKey key = new MemoryKey(request.getType(), request.getSeries());
        final AsyncFuture<MetricCollection> metrics = doFetch(key, request.getRange(), watcher);
        metrics.directTransform(mc -> {
            metricsConsumer.accept(mc);
            return mc;
        });
        return metrics.directTransform(mc -> FetchData.result(w.end())).directTransform(result -> {
            log.info("Read finished in " + result.getTrace().elapsed());
            return result;
        });
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
    public AsyncFuture<Void> deleteKey(BackendKey backendKey, QueryOptions options) {
        MemoryKey key = new MemoryKey(backendKey.getType(), backendKey.getSeries());
        // TODO
        // AsyncFuture<Collection<Long>> times = timeIndex.get(key);
        // For every entry in times, remove from 'metrics'

        return async.resolved();
    }

    @NoArgsConstructor
    @RequiredArgsConstructor
    @Data
    public static final class MemoryKey implements DataSerializable {
        @NonNull
        private MetricType source;
        @NonNull
        private Series series;

        public void hashTo(final ObjectHasher hasher) {
            hasher.putObject(getClass(), () -> {
                hasher.putField("source", source, hasher.enumValue());
                hasher.putField("series", series, hasher.with(Series::hashTo));
            });
        }

        @Override
        public void writeData(final ObjectDataOutput out) throws IOException {
            out.writeUTF(source.identifier());
            series.writeData(out);
        }

        @Override
        public void readData(final ObjectDataInput in) throws IOException {
            String sourceIdentifier = in.readUTF();
            source = MetricType
                .fromIdentifier(sourceIdentifier)
                .orElseThrow(
                    () -> new IllegalArgumentException("Unexpected data when deserializing"));
            series = Series.empty();
            series.readData(in);
        }
    }

    private AsyncFuture<WriteMetric> writeOne(final WriteMetric.Request request) {
        final MetricCollection g = request.getData();

        final MemoryKey key = new MemoryKey(g.getType(), request.getSeries());

        IMap<Long, Double> map = getMapForKey(key);
        RequestTimer<WriteMetric> timer = WriteMetric.timer();

        List<AsyncFuture<Void>> metricWriteFutures = new ArrayList<>();
        for (final Metric m : g.getData()) {
            metricWriteFutures.add(bind(map.setAsync(m.getTimestamp(), ((Point) m).getValue())));
        }

        AsyncFuture<Void> metricWriteFuture = async.collectAndDiscard(metricWriteFutures);

        return metricWriteFuture
            .directTransform(result -> timer.end())
            .directTransform(requestTimer -> {
                log.info("Write finished in " + requestTimer.getTimes().toString());
                return requestTimer;
            });
    }

    private AsyncFuture<MetricCollection> doFetch(
        final MemoryKey key, final DateRange range, final FetchQuotaWatcher watcher
    ) {
        IMap<Long, Double> map = getMapForKey(key);

        PredicateBuilder builder = new PredicateBuilder();
        EntryObject e = builder.getEntryObject();
        Predicate<Long, Double> predicate =
            e.key().greaterThan(range.start()).and(e.lessEqual(range.end()));

        AsyncFuture<Set<Map.Entry<Long, Double>>> getfuture = async.call(() -> {
            return map.entrySet(predicate);
        });

        List<Metric> data = new ArrayList<>();
        return getfuture.directTransform(result -> {
            data.addAll(result
                .stream()
                .map(entry -> new Point(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));
            sortMetricsByTime(data);
            return MetricCollection.build(MetricType.POINT, data);
        });
    }

    private void sortMetricsByTime(final List<Metric> data) {
        Collections.sort(data, new Comparator<Metric>() {
            @Override
            public int compare(final Metric m1, final Metric m2) {
                if (m1.getTimestamp() < m2.getTimestamp()) {
                    return -1;
                } else if (m1.getTimestamp() > m2.getTimestamp()) {
                    return 1;
                }
                return 0;
            }
        });
    }

    private IMap<Long, Double> getMapForKey(final MemoryKey key) {
        ObjectHasher objectHasher = new ObjectHasher(HASH_FUNCTION.newHasher());
        key.hashTo(objectHasher);
        final String keyHash = objectHasher.getHasher().hash().toString();
        return instance.getMap(keyHash);
    }

    private <T> AsyncFuture<T> bind(ICompletableFuture<T> hazelcastFuture) {
        ResolvableFuture<T> future = async.future();

        hazelcastFuture.andThen(new ExecutionCallback<T>() {
            @Override
            public void onResponse(final T response) {
                future.resolve(response);
            }

            @Override
            public void onFailure(final Throwable t) {
                future.fail(t);
            }
        });

        return future;
    }
}
