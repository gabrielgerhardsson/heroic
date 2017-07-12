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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
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
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.StreamCollector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
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

    private final Object createLock = new Object();

    private final AsyncFramework async;
    private final Groups groups;
    private final AsyncMultimap<MemoryKey, Long> timeIndex;
    private final AsyncMultimap<Long, Metric> metrics;

    @Inject
    public MemoryBackend(
        final AsyncFramework async, final Groups groups,
        @Named("storageTimeIndex") final AsyncMultimap<MemoryKey, Long> storageIndex,
        @Named("storageMetrics") final AsyncMultimap<Long, Metric> storageMetrics,
        LifeCycleRegistry registry
    ) {
        super(async);
        this.async = async;
        this.groups = groups;
        this.timeIndex = storageIndex;
        this.metrics = storageMetrics;
    }

    @Override
    public Statistics getStatistics() {
        try {
            return Statistics.of(MEMORY_KEYS, metrics.size().get());
        } catch (Exception e) {
            log.error("Async call failed: " + e);
            throw new RuntimeException(e);
        }
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
        return writeOne(request);
    }

    @Override
    public AsyncFuture<FetchData> fetch(FetchData.Request request, FetchQuotaWatcher watcher) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(FETCH);
        final MemoryKey key = new MemoryKey(request.getType(), request.getSeries());
        final AsyncFuture<MetricCollection> metrics = doFetch(key, request.getRange(), watcher);
        return metrics.directTransform(
            mc -> FetchData.of(w.end(), ImmutableList.of(), ImmutableList.of(mc))).directTransform(result -> {
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
        RequestTimer<WriteMetric> timer = WriteMetric.timer();

        Set<Long> bases = new HashSet<>();

        StreamCollector<Boolean, Boolean> collector = new StreamCollector<Boolean, Boolean>() {
            List<Boolean> results = Collections.synchronizedList(new ArrayList<>());
            List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

            @Override
            public void resolved(final Boolean result) throws Exception {
                results.add(result);
            }

            @Override
            public void failed(final Throwable cause) throws Exception {
                errors.add(cause);
                log.error("Write call failed: ", cause);
            }

            @Override
            public void cancelled() throws Exception {

            }

            @Override
            public Boolean end(final int resolved, final int failed, final int cancelled)
                throws Exception {
                // FIXME
                return true;
            }
        };

        List<Callable<AsyncFuture<Boolean>>> metricWriteCalls = new ArrayList<>();
        for (final Metric m : g.getData()) {
            long baseTs = getTimeBaseStart(m.getTimestamp());
            bases.add(baseTs);

            long hash = calculateHash(key, baseTs);
            metricWriteCalls.add(() -> metrics.put(hash, m));
        }
        AsyncFuture<Boolean> metricWriteFuture =
            async.eventuallyCollect(metricWriteCalls, collector, WRITE_PARALLELISM);

        return metricWriteFuture.directTransform(result -> timer.end()).directTransform(requestTimer -> {
            log.info("Write finished in " + requestTimer.getTimes().toString());
            return requestTimer;
        });

        /*
        return metricWriteFuture.lazyTransform((result) -> {
            if (!result) {
                // TODO
            }

            List<AsyncFuture<Boolean>> baseWrites = new ArrayList<AsyncFuture<Boolean>>();
            for (Long base : bases) {
                baseWrites.add(timeIndex.put(key, base));
            }

            StreamCollector<Boolean, Boolean> baseCollector =
                new StreamCollector<Boolean, Boolean>() {
                    List<Boolean> results = Collections.synchronizedList(new ArrayList<>());
                    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

                    @Override
                    public void resolved(final Boolean result) throws Exception {
                        results.add(result);
                    }

                    @Override
                    public void failed(final Throwable cause) throws Exception {
                        errors.add(cause);
                        log.error("Base write call failed: ", cause);
                    }

                    @Override
                    public void cancelled() throws Exception {

                    }

                    @Override
                    public Boolean end(final int resolved, final int failed, final int cancelled)
                        throws Exception {
                        // FIXME
                        return true;
                    }
                };

            return async.collect(baseWrites, baseCollector);

            // FIXME: Propagate captured errors
        }).directTransform(result -> timer.end());
        */
    }

    private AsyncFuture<MetricCollection> doFetch(
        final MemoryKey key, final DateRange range, final FetchQuotaWatcher watcher
    ) {
        AsyncFuture<List<Collection<Metric>>> chunks = fetchWholeChunks(key, range);
        List<Metric> data = new ArrayList<>();

        return chunks.directTransform(list -> {
            final int numberOfMetricsRead =
                list.stream().map(Collection::size).mapToInt(Integer::intValue).sum();
            watcher.readData(numberOfMetricsRead);

            sortCollectionsList(list);
            removeEmptyCollections(list);

            if (list.size() == 0) {
                return MetricCollection.empty();
            } else if (list.size() == 1) {
                for (final Metric m : list.get(0)) {
                    if (m.getTimestamp() > range.start() && m.getTimestamp() <= range.end()) {
                        data.add(m);
                    }
                }
                sortMetricsByTime(data);
                return MetricCollection.build(key.getSource(), data);
            }

            final Collection<Metric> first = list.get(0);
            for (final Metric m : first) {
                // Non-inclusive start of range
                if (m.getTimestamp() <= range.getStart()) {
                    continue;
                }
                data.add(m);
            }
            int index = 1;
            for (; index < list.size() - 1; index++) {
                data.addAll(list.get(index));
            }
            final Collection<Metric> last = list.get(index);
            for (final Metric m : last) {
                // Inclusive end of range
                if (m.getTimestamp() > range.getStart()) {
                    continue;
                }
                data.add(m);
            }

            sortMetricsByTime(data);

            return MetricCollection.build(key.getSource(), data);
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

    private void sortCollectionsList(final List<Collection<Metric>> list) {
        Collections.sort(list, (c1, c2) -> {
            if (c1.size() == 0 || c2.size() == 0) {
                if (c1.size() == 0 && c2.size() == 0) {
                    return 0;
                } else if (c1.size() == 0) {
                    return -1;
                } else {
                    return 1;
                }
            }
            final long ts1 = c1.iterator().next().getTimestamp();
            final long ts2 = c2.iterator().next().getTimestamp();
            if (ts1 < ts2) {
                return -1;
            } else if (ts1 > ts2) {
                return 1;
            }
            return 0;
        });
    }

    private void removeEmptyCollections(final List<Collection<Metric>> list) {
        // Remove empty collections - will always be at the start of the list
        while (list.size() > 0 && list.get(0).size() == 0) {
            list.remove(0);
        }
    }

    private AsyncFuture<List<Collection<Metric>>> fetchWholeChunks(MemoryKey key, DateRange range) {
        final List<AsyncFuture<Collection<Metric>>> allChunkFutures = new ArrayList<>();
        final long startBase = getTimeBaseStart(range.getStart());
        final long endBase = getTimeBaseStart(range.getEnd());

        List<Callable<AsyncFuture<Collection<Metric>>>> allChunkCallables = new ArrayList<>();
        for (long base = startBase; base <= endBase; base += CHUNK_SIZE_MS) {
            final Long finalBase = base;
            allChunkCallables.add(() -> fetchWholeChunk(key, finalBase));
        }

        final StreamCollector<Collection<Metric>, List<Collection<Metric>>> collector =
            new StreamCollector<Collection<Metric>, List<Collection<Metric>>>() {
                List<Collection<Metric>> results = Collections.synchronizedList(new ArrayList<>());
                List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

                @Override
                public void resolved(final Collection<Metric> result) throws Exception {
                    results.add(result);
                }

                @Override
                public void failed(final Throwable cause) throws Exception {
                    errors.add(cause);
                    log.error("Base write call failed: ", cause);
                }

                @Override
                public void cancelled() throws Exception {

                }

                @Override
                public List<Collection<Metric>> end(
                    final int resolved, final int failed, final int cancelled
                ) throws Exception {
                    return results;
                }
            };

        return async.eventuallyCollect(allChunkCallables, collector, READ_PARALLELISM);
    }

    private AsyncFuture<Collection<Metric>> fetchWholeChunk(
        final MemoryKey key, final long baseTs
    ) {
        final long hash = calculateHash(key, baseTs);
        return metrics.get(hash);
    }

    long getTimeBaseStart(long timestampMs) {
        return timestampMs - (timestampMs % CHUNK_SIZE_MS);
    }

    private long calculateHash(MemoryKey key, long baseTs) {
        Hasher hasher = HASH_FUNCTION.newHasher();
        ObjectHasher objectHasher = new ObjectHasher(hasher);

        key.hashTo(objectHasher);
        objectHasher.putField("base", baseTs, objectHasher.longValue());

        return hasher.hash().asLong();
    }
}
