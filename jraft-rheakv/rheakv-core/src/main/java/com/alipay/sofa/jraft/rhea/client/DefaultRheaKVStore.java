/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rhea.client;


import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.DescriberManager;
import com.alipay.sofa.jraft.rhea.FollowerStateListener;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.RegionEngine;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.StateListenerContainer;
import com.alipay.sofa.jraft.rhea.StoreEngine;

import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.failover.ListRetryCallable;
import com.alipay.sofa.jraft.rhea.client.failover.RetryCallable;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.client.failover.impl.BoolFailoverFuture;
import com.alipay.sofa.jraft.rhea.client.failover.impl.FailoverClosureImpl;
import com.alipay.sofa.jraft.rhea.client.failover.impl.ListFailoverFuture;
import com.alipay.sofa.jraft.rhea.client.failover.impl.MapFailoverFuture;
import com.alipay.sofa.jraft.rhea.client.pd.FakePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.RemotePlacementDriverClient;
import com.alipay.sofa.jraft.rhea.cmd.store.*;
import com.alipay.sofa.jraft.rhea.errors.ApiExceptionHelper;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.ErrorsHelper;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.metrics.KVMetricNames;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.*;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.storage.*;
import com.alipay.sofa.jraft.rhea.util.*;
import com.alipay.sofa.jraft.rhea.util.concurrent.AffinityNamedThreadFactory;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.Dispatcher;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.TaskDispatcher;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.WaitStrategyType;
import com.alipay.sofa.jraft.util.*;
import com.codahale.metrics.Histogram;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alipay.sofa.jraft.rhea.metadata.Region.ANY_REGION_ID;

/**
 * Default client of RheaKV store implementation.
 *
 * For example, the processing flow of the method {@link #scan(byte[], byte[])},
 * and the implementation principle of failover:
 *
 * <pre>
 * 1. The first step is to filter out region1, region2, and region3 from the routing table.
 *
 *                ┌─────────────────┐                               ┌─────────────────┐
 *                │  scan startKey  │                               │   scan endKey   │
 *                └────────┬────────┘                               └────────┬────────┘
 *                         │                                                 │
 *                         │                                                 │
 *                         │                                                 │
 * ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─     │  ┌ ─ ─ ─ ─ ─ ─ ┐           ┌ ─ ─ ─ ─ ─ ─ ┐      │    ┌ ─ ─ ─ ─ ─ ─ ┐
 *  startKey1=byte[0] │    │     startKey2                 startKey3         │       startKey4
 * └ ─ ─ ─ ┬ ─ ─ ─ ─ ─     │  └ ─ ─ ─│─ ─ ─ ┘           └ ─ ─ ─│─ ─ ─ ┘      │    └ ─ ─ ─│─ ─ ─ ┘
 *         │               │         │                         │             │           │
 *         ▼───────────────▼─────────▼─────────────────────────▼─────────────▼───────────▼─────────────────────────┐
 *         │                         │                         │                         │                         │
 *         │                         │                         │                         │                         │
 *         │         region1         │         region2         │          region3        │         region4         │
 *         │                         │                         │                         │                         │
 *         └─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
 *
 * 2. The second step is to split the request(scan -> multi-region scan):
 *          region1->regionScan(startKey, regionEndKey1)
 *          region2->regionScan(regionStartKey2, regionEndKey2)
 *          region3->regionScan(regionStartKey3, endKey)
 *
 *            ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─      ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *                call region1   │        call region2   │         call region3   │
 *            └ ─ ─ ─ ─ ─ ─ ─ ─ ─     └ ─ ─ ─ ─ ─ ─ ─ ─ ─      └ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *                     ║                       ║                        ║
 *
 *                     ║                       ║                        ║
 *                     ▽                       ▽                        ▽
 *     ┌─────────────────────────┬─────────────────────────┬─────────────────────────┬─────────────────────────┐
 *     │                         │                         │                         │                         │
 *     │                         │                         │                         │                         │
 *     │         region1         │         region2         │          region3        │         region4         │
 *     │                         │                         │                         │                         │
 *     └─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
 *
 * 3. The third step, encountering the region split (the sign of the split is the change of the region epoch)
 *      To refresh the RegionRouteTable, you need to obtain the latest routing table from the PD.
 *
 *      For example, region2 is split into region2 + region5:
 *          The request 'region2->regionScan(regionStartKey2, regionEndKey2)' split and retry
 *              1. region2->regionScan(regionStartKey2, newRegionEndKey2)
 *              2. region5->regionScan(regionStartKey5, regionEndKey5)
 *
 *            ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─                              ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *                call region1   │                                 call region3   │
 *            └ ─ ─ ─ ─ ─ ─ ─ ─ ─                              └ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *                     ║        ┌ ─ ─ ─ ─ ─ ─ ┐                         ║
 *                               retry region2
 *                     ║        └ ─ ─ ─ ─ ─ ─ ┘┌ ─ ─ ─ ─ ─ ─ ┐          ║
 *                                     ║        retry region5
 *                     ║                       └ ─ ─ ─ ─ ─ ─ ┘          ║
 *                                     ║              ║
 *                     ║                                                ║
 *                     ▽               ▽              ▽                 ▽
 *     ┌─────────────────────────┬────────────┬ ─ ─ ─ ─ ─ ─┌─────────────────────────┬─────────────────────────┐
 *     │                         │            │            │                         │                         │
 *     │                         │            │            │                         │                         │
 *     │         region1         │  region2   │  region5   │          region3        │         region4         │
 *     │                         │            │            │                         │                         │
 *     └─────────────────────────┴────────────┘─ ─ ─ ─ ─ ─ ┴─────────────────────────┴─────────────────────────┘
 *
 * 4. Encountering 'Invalid-Peer'(NOT_LEADER, NO_REGION_FOUND, LEADER_NOT_AVAILABLE)
 *      This is very simple, re-acquire the latest leader of the raft-group to which the current key belongs,
 *      and then call again.
 * </pre>
 *
 * @author jiachun.fjc
 */
public class DefaultRheaKVStore implements RheaKVStore {

    private static final Logger                LOG                    = LoggerFactory
                                                                          .getLogger(DefaultRheaKVStore.class);

    static {
        ExtSerializerSupports.init();
    }

    private final StateListenerContainer<Long> stateListenerContainer = new StateListenerContainer<>();
    private StoreEngine                        storeEngine;
    private PlacementDriverClient              pdClient;
    private RheaKVRpcService                   rheaKVRpcService;
    private RheaKVStoreOptions                 opts;
    private int                                failoverRetries;
    private long                               futureTimeoutMillis;
    private boolean                            onlyLeaderRead;
    private Dispatcher                         kvDispatcher;
    private BatchingOptions                    batchingOpts;
    private GetBatching                        getBatching;
    private GetBatching                        getBatchingOnlySafe;
    private PutBatching                        putBatching;
    private CompositeBatching                  compositeBatching;

    private volatile boolean                   started;
    private boolean                            hashRegioned;

    @Override
    public synchronized boolean init(final RheaKVStoreOptions opts) {
        if (this.started) {
            LOG.info("[DefaultRheaKVStore] already started.");
            return true;
        }

        DescriberManager.getInstance().addDescriber(RouteTable.getInstance());

        this.opts = opts;
        // init placement driver
        final PlacementDriverOptions pdOpts = opts.getPlacementDriverOptions();
        final String clusterName = opts.getClusterName();
        Requires.requireNonNull(pdOpts, "opts.placementDriverOptions");
        Requires.requireNonNull(clusterName, "opts.clusterName");
        if (Strings.isBlank(pdOpts.getInitialServerList())) {
            // if blank, extends parent's value
            pdOpts.setInitialServerList(opts.getInitialServerList());
        }
        if (pdOpts.isFake()) {
            this.pdClient = new FakePlacementDriverClient(opts.getClusterId(), clusterName);
        } else {
            this.pdClient = new RemotePlacementDriverClient(opts.getClusterId(), clusterName);
        }
        if (!this.pdClient.init(pdOpts)) {
            LOG.error("Fail to init [PlacementDriverClient].");
            return false;
        }
        // init store engine
        final StoreEngineOptions stOpts = opts.getStoreEngineOptions();
        if (stOpts != null) {
            stOpts.setInitialServerList(opts.getInitialServerList());
            this.storeEngine = new StoreEngine(this.pdClient, this.stateListenerContainer);
            if (!this.storeEngine.init(stOpts)) {
                LOG.error("Fail to init [StoreEngine].");
                return false;
            }
        }
        final Endpoint selfEndpoint = this.storeEngine == null ? null : this.storeEngine.getSelfEndpoint();
        final RpcOptions rpcOpts = opts.getRpcOptions();
        Requires.requireNonNull(rpcOpts, "opts.rpcOptions");
        this.rheaKVRpcService = new DefaultRheaKVRpcService(this.pdClient, selfEndpoint) {

            @Override
            public Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
                final Endpoint leader = getLeaderByRegionEngine(regionId);
                if (leader != null) {
                    return leader;
                }
                return super.getLeader(regionId, forceRefresh, timeoutMillis);
            }
        };
        if (!this.rheaKVRpcService.init(rpcOpts)) {
            LOG.error("Fail to init [RheaKVRpcService].");
            return false;
        }
        this.failoverRetries = opts.getFailoverRetries();
        this.futureTimeoutMillis = opts.getFutureTimeoutMillis();
        this.onlyLeaderRead = opts.isOnlyLeaderRead();
        if (opts.isUseParallelKVExecutor()) {
            final int numWorkers = Utils.cpus();
            final int bufSize = numWorkers << 4;
            final String name = "parallel-kv-executor";
            final ThreadFactory threadFactory = Constants.THREAD_AFFINITY_ENABLED
                    ? new AffinityNamedThreadFactory(name, true) : new NamedThreadFactory(name, true);
            this.kvDispatcher = new TaskDispatcher(bufSize, numWorkers, WaitStrategyType.LITE_BLOCKING_WAIT, threadFactory);
        }
        this.batchingOpts = opts.getBatchingOptions();
        if (this.batchingOpts.isAllowBatching()) {
            this.getBatching = new GetBatching(KeyEvent::new, "get_batching",
                    new GetBatchingHandler("get", false));
            this.getBatchingOnlySafe = new GetBatching(KeyEvent::new, "get_batching_only_safe",
                    new GetBatchingHandler("get_only_safe", true));
            this.putBatching = new PutBatching(KVEvent::new, "put_batching",
                    new PutBatchingHandler("put"));
            this.compositeBatching = new CompositeBatching(KVCompositeEvent::new, "composite_batching",
                    new CompositeBatchingHandler("batch"));
        }
        LOG.info("[DefaultRheaKVStore] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        this.started = false;
        if (this.pdClient != null) {
            this.pdClient.shutdown();
        }
        if (this.storeEngine != null) {
            this.storeEngine.shutdown();
        }
        if (this.rheaKVRpcService != null) {
            this.rheaKVRpcService.shutdown();
        }
        if (this.kvDispatcher != null) {
            this.kvDispatcher.shutdown();
        }
        if (this.getBatching != null) {
            this.getBatching.shutdown();
        }
        if (this.getBatchingOnlySafe != null) {
            this.getBatchingOnlySafe.shutdown();
        }
        if (this.putBatching != null) {
            this.putBatching.shutdown();
        }
        if (this.compositeBatching != null) {
            this.compositeBatching.shutdown();
        }
        this.stateListenerContainer.clear();
        LOG.warn("[DefaultRheaKVStore] shutdown successfully.");
    }

    @Override
    public boolean isStarted() {
        return this.started;
    }

    private void checkRegionRoute(boolean isHashRouted) {
        if (this.opts.getPlacementDriverOptions().getHashRoute() != isHashRouted) {
            throw new RheaRuntimeException("[DefaultRheaKVStore] invalid invoke path with isHashRouted = "
                                           + isHashRouted);
        }
    }

    /**
     * Returns a heap-allocated iterator over the contents of the
     * database.
     * <p>
     * Caller should close the iterator when it is no longer needed.
     * The returned iterator should be closed before this db is closed.
     * <p>
     * <pre>
     *     KVIterator it = unsafeLocalIterator();
     *     try {
     *         // do something
     *     } finally {
     *         it.close();
     *     }
     * <pre/>
     */
    public KVIterator unsafeLocalIterator() {
        checkState();
        if (this.pdClient instanceof RemotePlacementDriverClient) {
            throw new UnsupportedOperationException("unsupported operation on multi-region");
        }
        if (this.storeEngine == null) {
            throw new IllegalStateException("current node do not have store engine");
        }
        // [TODO] support aggregated iterator on multiple regions
        return this.storeEngine.getRawKVStore(ANY_REGION_ID).localIterator();
    }

    @Override
    public CompletableFuture<byte[]> get(final byte[] key) {
        return get(key, true);
    }

    @Override
    public CompletableFuture<byte[]> get(final long regionId, final byte[] key) {
        Requires.requireNonNull(key, "key");
        return get(key, true, new CompletableFuture<>(), true, regionId);
    }

    @Override
    public CompletableFuture<byte[]> get(final String key) {
        return get(BytesUtil.writeUtf8(key));
    }

    @Override
    public CompletableFuture<byte[]> get(final long regionId, final String key) {
        return get(regionId, BytesUtil.writeUtf8(key));
    }

    @Override
    public CompletableFuture<byte[]> get(final byte[] key, final boolean readOnlySafe) {
        Requires.requireNonNull(key, "key");
        return get(key, readOnlySafe, new CompletableFuture<>(), true, ANY_REGION_ID);
    }

    @Override
    public CompletableFuture<byte[]> get(final long regionId, final byte[] key, final boolean readOnlySafe) {
        Requires.requireNonNull(key, "key");
        return get(key, readOnlySafe, new CompletableFuture<>(), true, regionId);
    }

    @Override
    public CompletableFuture<byte[]> get(final String key, final boolean readOnlySafe) {
        return get(BytesUtil.writeUtf8(key), readOnlySafe);
    }

    @Override
    public CompletableFuture<byte[]> get(final long regionId, final String key, final boolean readOnlySafe) {
        return get(regionId, BytesUtil.writeUtf8(key), readOnlySafe);
    }

    @Override
    public byte[] bGet(final byte[] key) {
        return FutureHelper.get(get(key), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final long regionId, final byte[] key) {
        return FutureHelper.get(get(regionId, key), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final String key) {
        return FutureHelper.get(get(key), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final long regionId, final String key) {
        return FutureHelper.get(get(regionId, key), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final byte[] key, final boolean readOnlySafe) {
        return FutureHelper.get(get(key, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final long regionId, final byte[] key, final boolean readOnlySafe) {
        return FutureHelper.get(get(regionId, key, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final String key, final boolean readOnlySafe) {
        return FutureHelper.get(get(key, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGet(final long regionId, final String key, final boolean readOnlySafe) {
        return FutureHelper.get(get(regionId, key, readOnlySafe), this.futureTimeoutMillis);
    }

    private CompletableFuture<byte[]> get(final byte[] key, final boolean readOnlySafe,
                                          final CompletableFuture<byte[]> future, final boolean tryBatching,
                                          final long regionId) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(key, "key");
        if (tryBatching) {
            final GetBatching getBatching = readOnlySafe ? this.getBatchingOnlySafe : this.getBatching;
            if (getBatching != null && getBatching.apply(regionId, key, future)) {
                return future;
            }
        }
        internalGet(key, readOnlySafe, future, this.failoverRetries, null, this.onlyLeaderRead, regionId);
        return future;
    }

    private void internalGet(final byte[] key, final boolean readOnlySafe, final CompletableFuture<byte[]> future,
                             final int retriesLeft, final Errors lastCause, final boolean requireLeader, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause)) : this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), requireLeader);
        // require leader on retry
        final RetryRunner retryRunner = retryCause -> internalGet(key, readOnlySafe, future, retriesLeft - 1,
                retryCause, true, regionId);
        final FailoverClosure<byte[]> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).get(key, readOnlySafe, closure);
            }
        } else {
            final GetRequest request = new GetRequest();
            request.setKey(key);
            request.setReadOnlySafe(readOnlySafe);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause, requireLeader);
        }
    }

    @Override
    public CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys) {
        return multiGet(keys, true);
    }

    @Override
    public CompletableFuture<Map<ByteArray, byte[]>> multiGet(final long regionId, final List<byte[]> keys) {
        return multiGet(regionId, keys, true);
    }

    @Override
    public CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys, final boolean readOnlySafe) {
        return multiGet(ANY_REGION_ID, keys, readOnlySafe);
    }

    @Override
    public CompletableFuture<Map<ByteArray, byte[]>> multiGet(final long regionId, final List<byte[]> keys,
                                                              final boolean readOnlySafe) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(keys, "keys");
        final FutureGroup<Map<ByteArray, byte[]>> futureGroup = internalMultiGet(keys, readOnlySafe,
            this.failoverRetries, null, regionId);
        return FutureHelper.joinMap(futureGroup, keys.size());
    }

    @Override
    public Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys) {
        return FutureHelper.get(multiGet(keys), this.futureTimeoutMillis);
    }

    @Override
    public Map<ByteArray, byte[]> bMultiGet(final long regionId, final List<byte[]> keys) {
        return FutureHelper.get(multiGet(regionId, keys), this.futureTimeoutMillis);
    }

    @Override
    public Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys, final boolean readOnlySafe) {
        return FutureHelper.get(multiGet(keys, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public Map<ByteArray, byte[]> bMultiGet(final long regionId, final List<byte[]> keys, final boolean readOnlySafe) {
        return FutureHelper.get(multiGet(regionId, keys, readOnlySafe), this.futureTimeoutMillis);
    }

    private FutureGroup<Map<ByteArray, byte[]>> internalMultiGet(final List<byte[]> keys, final boolean readOnlySafe,
                                                                 final int retriesLeft, final Throwable lastCause, final long regionId) {
        Map<Region, List<byte[]>> regionMap;
        if (regionId == ANY_REGION_ID) {
            regionMap = this.pdClient.findRegionsByKeys(keys, ApiExceptionHelper.isInvalidEpoch(lastCause));
        } else {
            regionMap = Maps.newHashMap();
            regionMap.put(this.pdClient.getRegionById(regionId), keys);
        }
        final List<CompletableFuture<Map<ByteArray, byte[]>>> futures = Lists.newArrayListWithCapacity(regionMap.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Map.Entry<Region, List<byte[]>> entry : regionMap.entrySet()) {
            final Region region = entry.getKey();
            final List<byte[]> subKeys = entry.getValue();
            final RetryCallable<Map<ByteArray, byte[]>> retryCallable = retryCause -> internalMultiGet(subKeys,
                    readOnlySafe, retriesLeft - 1, retryCause, regionId);
            final MapFailoverFuture<ByteArray, byte[]> future = new MapFailoverFuture<>(retriesLeft, retryCallable);
            internalRegionMultiGet(region, subKeys, readOnlySafe, future, retriesLeft, lastError, this.onlyLeaderRead);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    private void internalRegionMultiGet(final Region region, final List<byte[]> subKeys, final boolean readOnlySafe,
                                        final CompletableFuture<Map<ByteArray, byte[]>> future, final int retriesLeft,
                                        final Errors lastCause, final boolean requireLeader) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), requireLeader);
        // require leader on retry
        final RetryRunner retryRunner = retryCause -> internalRegionMultiGet(region, subKeys, readOnlySafe, future,
                retriesLeft - 1, retryCause, true);
        final FailoverClosure<Map<ByteArray, byte[]>> closure = new FailoverClosureImpl<>(future,
                false, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (this.kvDispatcher == null) {
                    rawKVStore.multiGet(subKeys, readOnlySafe, closure);
                } else {
                    this.kvDispatcher.execute(() -> rawKVStore.multiGet(subKeys, readOnlySafe, closure));
                }
            }
        } else {
            final MultiGetRequest request = new MultiGetRequest();
            request.setKeys(subKeys);
            request.setReadOnlySafe(readOnlySafe);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause, requireLeader);
        }
    }

    @Override
    public CompletableFuture<Boolean> containsKey(final byte[] key) {
        checkState();
        Requires.requireNonNull(key, "key");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalContainsKey(key, future, this.failoverRetries, null, ANY_REGION_ID);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> containsKey(final long regionId, final byte[] key) {
        checkState();
        Requires.requireNonNull(key, "key");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalContainsKey(key, future, this.failoverRetries, null, regionId);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> containsKey(final String key) {
        return containsKey(BytesUtil.writeUtf8(key));
    }

    @Override
    public CompletableFuture<Boolean> containsKey(final long regionId, final String key) {
        return containsKey(regionId, BytesUtil.writeUtf8(key));
    }

    @Override
    public Boolean bContainsKey(final byte[] key) {
        return FutureHelper.get(containsKey(key), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bContainsKey(final long regionId, final byte[] key) {
        return FutureHelper.get(containsKey(regionId, key), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bContainsKey(final String key) {
        return FutureHelper.get(containsKey(key), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bContainsKey(final long regionId, final String key) {
        return FutureHelper.get(containsKey(regionId, key), this.futureTimeoutMillis);
    }

    private void internalContainsKey(final byte[] key, final CompletableFuture<Boolean> future,
                                     final int retriesLeft, final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalContainsKey(key, future, retriesLeft - 1,
                retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).containsKey(key, closure);
            }
        } else {
            final ContainsKeyRequest request = new ContainsKeyRequest();
            request.setKey(key);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey) {
        return scan(startKey, endKey, true);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final long regionId, final byte[] startKey, final byte[] endKey) {
        return scan(regionId, startKey, endKey, true);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey) {
        return scan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey));
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final long regionId, final String startKey, final String endKey) {
        return scan(regionId, BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey));
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe) {
        return scan(startKey, endKey, readOnlySafe, true);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final long regionId, final byte[] startKey, final byte[] endKey,
                                                 final boolean readOnlySafe) {
        return scan(regionId, startKey, endKey, readOnlySafe, true);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey, final boolean readOnlySafe) {
        return scan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), readOnlySafe);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final long regionId, final String startKey, final String endKey,
                                                 final boolean readOnlySafe) {
        return scan(regionId, BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), readOnlySafe);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey,
                                                 final boolean readOnlySafe, final boolean returnValue) {
        return scan(ANY_REGION_ID, startKey, endKey, readOnlySafe, returnValue);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final long regionId, final byte[] startKey, final byte[] endKey,
                                                 final boolean readOnlySafe, final boolean returnValue) {
        checkState();
        final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
        if (endKey != null) {
            Requires.requireTrue(BytesUtil.compare(realStartKey, endKey) < 0, "startKey must < endKey");
        }
        final FutureGroup<List<KVEntry>> futureGroup = internalScan(realStartKey, endKey, readOnlySafe, returnValue,
            this.failoverRetries, null, regionId);
        return FutureHelper.joinList(futureGroup);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey,
                                                 final boolean readOnlySafe, final boolean returnValue) {
        return scan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), readOnlySafe, returnValue);
    }

    @Override
    public CompletableFuture<List<KVEntry>> scan(final long regionId, final String startKey, final String endKey,
                                                 final boolean readOnlySafe, final boolean returnValue) {
        return scan(regionId, BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), readOnlySafe, returnValue);
    }

    @Override
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey) {
        return FutureHelper.get(scan(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final long regionId, final byte[] startKey, final byte[] endKey) {
        return FutureHelper.get(scan(regionId, startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final String startKey, final String endKey) {
        return FutureHelper.get(scan(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final long regionId, final String startKey, final String endKey) {
        return FutureHelper.get(scan(regionId, startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe) {
        return FutureHelper.get(scan(startKey, endKey, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final long regionId, final byte[] startKey, final byte[] endKey,
                               final boolean readOnlySafe) {
        return FutureHelper.get(scan(regionId, startKey, endKey, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe) {
        return FutureHelper.get(scan(startKey, endKey, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final long regionId, final String startKey, final String endKey,
                               final boolean readOnlySafe) {
        return FutureHelper.get(scan(regionId, startKey, endKey, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                               final boolean returnValue) {
        return FutureHelper.get(scan(startKey, endKey, readOnlySafe, returnValue), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final long regionId, final byte[] startKey, final byte[] endKey,
                               final boolean readOnlySafe, final boolean returnValue) {
        return FutureHelper.get(scan(regionId, startKey, endKey, readOnlySafe, returnValue), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe,
                               final boolean returnValue) {
        return FutureHelper.get(scan(startKey, endKey, readOnlySafe, returnValue), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bScan(final long regionId, final String startKey, final String endKey,
                               final boolean readOnlySafe, final boolean returnValue) {
        return FutureHelper.get(scan(regionId, startKey, endKey, readOnlySafe, returnValue), this.futureTimeoutMillis);
    }

    private FutureGroup<List<KVEntry>> internalScan(final byte[] startKey, final byte[] endKey,
                                                    final boolean readOnlySafe, final boolean returnValue,
                                                    final int retriesLeft, final Throwable lastCause, final long regionId) {
        Requires.requireNonNull(startKey, "startKey");

        final List<Region> regionList = (regionId == ANY_REGION_ID) ?
                this.pdClient.findRegionsByKeyRange(startKey, endKey, ApiExceptionHelper.isInvalidEpoch(lastCause)) :
                Lists.newArrayList(this.pdClient.getRegionById(regionId));

        final List<CompletableFuture<List<KVEntry>>> futures = Lists.newArrayListWithCapacity(regionList.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Region region : regionList) {
            final byte[] regionStartKey = region.getStartKey();
            final byte[] regionEndKey = region.getEndKey();
            final byte[] subStartKey = (regionId != ANY_REGION_ID || regionStartKey == null) ? startKey : BytesUtil.max(regionStartKey, startKey);
            final byte[] subEndKey = (regionId != ANY_REGION_ID || regionEndKey == null) ? endKey :
                    (endKey == null ? regionEndKey : BytesUtil.min(regionEndKey, endKey));
            final ListRetryCallable<KVEntry> retryCallable = retryCause -> internalScan(subStartKey, subEndKey,
                    readOnlySafe, returnValue, retriesLeft - 1, retryCause, regionId);
            final ListFailoverFuture<KVEntry> future = new ListFailoverFuture<>(retriesLeft, retryCallable);
            internalRegionScan(region, subStartKey, subEndKey, false, readOnlySafe, returnValue, future, retriesLeft,
                    lastError, this.onlyLeaderRead);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    private void internalRegionScan(final Region region, final byte[] subStartKey, final byte[] subEndKey,
                                    final boolean reverse, final boolean readOnlySafe, final boolean returnValue,
                                    final CompletableFuture<List<KVEntry>> future, final int retriesLeft,
                                    final Errors lastCause, final boolean requireLeader) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), requireLeader);
        // require leader on retry
        final RetryRunner retryRunner = retryCause -> internalRegionScan(region, subStartKey, subEndKey, reverse, readOnlySafe,
                returnValue, future, retriesLeft - 1, retryCause, true);
        final FailoverClosure<List<KVEntry>> closure = new FailoverClosureImpl<>(future, false,
                retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (reverse) {
                    if (this.kvDispatcher == null) {
                        rawKVStore.reverseScan(subStartKey, subEndKey, readOnlySafe, returnValue, closure);
                    } else {
                        this.kvDispatcher.execute(
                                () -> rawKVStore.reverseScan(subStartKey, subEndKey, readOnlySafe, returnValue, closure));
                    }
                } else {
                    if (this.kvDispatcher == null) {
                        rawKVStore.scan(subStartKey, subEndKey, readOnlySafe, returnValue, closure);
                    } else {
                        this.kvDispatcher.execute(
                                () -> rawKVStore.scan(subStartKey, subEndKey, readOnlySafe, returnValue, closure));
                    }
                }
            }
        } else {
            final ScanRequest request = new ScanRequest();
            request.setStartKey(subStartKey);
            request.setEndKey(subEndKey);
            request.setReadOnlySafe(readOnlySafe);
            request.setReturnValue(returnValue);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            request.setReverse(reverse);
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause, requireLeader);
        }
    }


    @Override
    public CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey) {
        return reverseScan(startKey, endKey, true);
    }

    @Override
    public CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey) {
        return reverseScan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey));
    }

    @Override
    public CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey,
                                                        final boolean readOnlySafe) {
        return reverseScan(startKey, endKey, readOnlySafe, true);
    }

    @Override
    public CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey,
                                                        final boolean readOnlySafe) {
        return reverseScan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), readOnlySafe);
    }

    @Override
    public CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey,
                                                        final boolean readOnlySafe, final boolean returnValue) {
        checkState();
        final byte[] realEndKey = BytesUtil.nullToEmpty(endKey);
        if (startKey != null) {
            Requires.requireTrue(BytesUtil.compare(startKey, realEndKey) > 0, "startKey must > endKey");
        }
        final FutureGroup<List<KVEntry>> futureGroup = internalReverseScan(startKey, realEndKey, readOnlySafe,
            returnValue, this.failoverRetries, null);
        return FutureHelper.joinList(futureGroup);
    }

    @Override
    public CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey,
                                                        final boolean readOnlySafe, final boolean returnValue) {
        return reverseScan(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), readOnlySafe, returnValue);
    }

    @Override
    public List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey) {
        return FutureHelper.get(reverseScan(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bReverseScan(final String startKey, final String endKey) {
        return FutureHelper.get(reverseScan(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe) {
        return FutureHelper.get(reverseScan(startKey, endKey, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bReverseScan(final String startKey, final String endKey, final boolean readOnlySafe) {
        return FutureHelper.get(reverseScan(startKey, endKey, readOnlySafe), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                                      final boolean returnValue) {
        return FutureHelper.get(reverseScan(startKey, endKey, readOnlySafe, returnValue), this.futureTimeoutMillis);
    }

    @Override
    public List<KVEntry> bReverseScan(final String startKey, final String endKey, final boolean readOnlySafe,
                                      final boolean returnValue) {
        return FutureHelper.get(reverseScan(startKey, endKey, readOnlySafe, returnValue), this.futureTimeoutMillis);
    }

    private FutureGroup<List<KVEntry>> internalReverseScan(final byte[] startKey, final byte[] endKey,
                                                           final boolean readOnlySafe, final boolean returnValue,
                                                           final int retriesLeft, final Throwable lastCause) {
        Requires.requireNonNull(endKey, "endKey");
        final List<Region> regionList = this.pdClient.findRegionsByKeyRange(endKey, startKey, ApiExceptionHelper.isInvalidEpoch(lastCause));
        Collections.reverse(regionList);
        final List<CompletableFuture<List<KVEntry>>> futures = Lists.newArrayListWithCapacity(regionList.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Region region : regionList) {
            final byte[] regionEndKey = region.getEndKey();
            final byte[] regionStartKey = region.getStartKey();
            final byte[] subStartKey = regionEndKey == null ? startKey : (startKey == null ? regionEndKey : BytesUtil.min(regionEndKey, startKey));
            final byte[] subEndKey = regionStartKey == null ? endKey : BytesUtil.max(regionStartKey, endKey);
            final ListRetryCallable<KVEntry> retryCallable = retryCause -> internalReverseScan(subStartKey, subEndKey,
                    readOnlySafe, returnValue, retriesLeft - 1, retryCause);
            final ListFailoverFuture<KVEntry> future = new ListFailoverFuture<>(retriesLeft, retryCallable);
            internalRegionScan(region, subStartKey, subEndKey, true, readOnlySafe, returnValue, future, retriesLeft,
                    lastError, this.onlyLeaderRead );
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    public List<KVEntry> singleScan(final byte[] startKey, final byte[] endKey, final int limit,
                                    final boolean readOnlySafe, final boolean returnValue, final long regionId) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
        if (endKey != null) {
            Requires.requireTrue(BytesUtil.compare(realStartKey, endKey) < 0, "startKey must < endKey");
        }
        Requires.requireTrue(limit > 0, "limit must > 0");
        final CompletableFuture<List<KVEntry>> future = new CompletableFuture<>();
        internalSingleRegionScan(realStartKey, endKey, limit, readOnlySafe, returnValue, future, this.failoverRetries,
            null, this.onlyLeaderRead, regionId);
        return FutureHelper.get(future, this.futureTimeoutMillis);
    }

    private void internalSingleRegionScan(final byte[] startKey, final byte[] endKey, final int limit,
                                          final boolean readOnlySafe, final boolean returnValue,
                                          final CompletableFuture<List<KVEntry>> future, final int retriesLeft,
                                          final Errors lastCause, final boolean requireLeader, final long regionId) {
        Requires.requireNonNull(startKey, "startKey");
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(startKey, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final byte[] regionEndKey = region.getEndKey();
        final byte[] realEndKey = (regionId != ANY_REGION_ID || regionEndKey == null) ? endKey :
                (endKey == null ? regionEndKey : BytesUtil.min(regionEndKey, endKey));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), requireLeader);
        // require leader on retry
        final RetryRunner retryRunner = retryCause -> internalSingleRegionScan(startKey, endKey, limit, readOnlySafe,
                returnValue, future, retriesLeft - 1, retryCause, true, regionId);
        final FailoverClosure<List<KVEntry>> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).scan(startKey, realEndKey, limit, readOnlySafe, returnValue, closure);
            }
        } else {
            final ScanRequest request = new ScanRequest();
            request.setStartKey(startKey);
            request.setEndKey(realEndKey);
            request.setLimit(limit);
            request.setReadOnlySafe(readOnlySafe);
            request.setReturnValue(returnValue);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause, requireLeader);
        }
    }

    @Override
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize) {
        return iterator(startKey, endKey, bufSize, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final long regionid, final byte[] startKey, final byte[] endKey,
                                          final int bufSize) {
        return iterator(regionid, startKey, endKey, bufSize, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize) {
        return iterator(startKey, endKey, bufSize, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final long regionId, final String startKey, final String endKey,
                                          final int bufSize) {
        return iterator(regionId, startKey, endKey, bufSize, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                          final boolean readOnlySafe) {
        return iterator(startKey, endKey, bufSize, readOnlySafe, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final long regionId, final byte[] startKey, final byte[] endKey,
                                          final int bufSize, final boolean readOnlySafe) {
        return iterator(regionId, startKey, endKey, bufSize, readOnlySafe, true);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                          final boolean readOnlySafe) {
        return iterator(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), bufSize, readOnlySafe);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final long regionId, final String startKey, final String endKey,
                                          final int bufSize, final boolean readOnlySafe) {
        return iterator(regionId, BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), bufSize, readOnlySafe);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                          final boolean readOnlySafe, final boolean returnValue) {
        return new DefaultRheaIterator(this, startKey, endKey, bufSize, readOnlySafe, returnValue);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final long regionId, final byte[] startKey, final byte[] endKey,
                                          final int bufSize, final boolean readOnlySafe, final boolean returnValue) {
        return new DefaultRheaIterator(this, startKey, endKey, bufSize, readOnlySafe, returnValue, regionId);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                          final boolean readOnlySafe, final boolean returnValue) {
        return iterator(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), bufSize, readOnlySafe, returnValue);
    }

    @Override
    public RheaIterator<KVEntry> iterator(final long regionId, final String startKey, final String endKey,
                                          final int bufSize, final boolean readOnlySafe, final boolean returnValue) {
        return iterator(regionId, BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey), bufSize, readOnlySafe,
            returnValue);
    }

    @Override
    public CompletableFuture<Sequence> getSequence(final byte[] seqKey, final int step) {
        return getSequence(ANY_REGION_ID, seqKey, step);
    }

    @Override
    public CompletableFuture<Sequence> getSequence(final long regionId, final byte[] seqKey, final int step) {
        checkState();
        Requires.requireNonNull(seqKey, "seqKey");
        Requires.requireTrue(step >= 0, "step must >= 0");
        final CompletableFuture<Sequence> future = new CompletableFuture<>();
        internalGetSequence(seqKey, step, future, this.failoverRetries, null, regionId);
        return future;
    }

    @Override
    public CompletableFuture<Sequence> getSequence(final String seqKey, final int step) {
        return getSequence(BytesUtil.writeUtf8(seqKey), step);
    }

    @Override
    public CompletableFuture<Sequence> getSequence(final long regionId, final String seqKey, final int step) {
        return getSequence(regionId, BytesUtil.writeUtf8(seqKey), step);
    }

    @Override
    public Sequence bGetSequence(final byte[] seqKey, final int step) {
        return FutureHelper.get(getSequence(seqKey, step), this.futureTimeoutMillis);
    }

    @Override
    public Sequence bGetSequence(final long regionId, final byte[] seqKey, final int step) {
        return FutureHelper.get(getSequence(regionId, seqKey, step), this.futureTimeoutMillis);
    }

    @Override
    public Sequence bGetSequence(final String seqKey, final int step) {
        return FutureHelper.get(getSequence(seqKey, step), this.futureTimeoutMillis);
    }

    @Override
    public Sequence bGetSequence(final long regionId, final String seqKey, final int step) {
        return FutureHelper.get(getSequence(regionId, seqKey, step), this.futureTimeoutMillis);
    }

    @Override
    public CompletableFuture<Long> getLatestSequence(final byte[] seqKey) {
        final CompletableFuture<Long> cf = new CompletableFuture<>();
        getSequence(seqKey, 0).whenComplete((sequence, throwable) -> {
            if (throwable == null) {
                cf.complete(sequence.getStartValue());
            } else {
                cf.completeExceptionally(throwable);
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Long> getLatestSequence(final long regionId, final byte[] seqKey) {
        final CompletableFuture<Long> cf = new CompletableFuture<>();
        getSequence(regionId, seqKey, 0).whenComplete((sequence, throwable) -> {
            if (throwable == null) {
                cf.complete(sequence.getStartValue());
            } else {
                cf.completeExceptionally(throwable);
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Long> getLatestSequence(final String seqKey) {
        return getLatestSequence(BytesUtil.writeUtf8(seqKey));
    }

    @Override
    public CompletableFuture<Long> getLatestSequence(final long regionId, final String seqKey) {
        return getLatestSequence(regionId, BytesUtil.writeUtf8(seqKey));
    }

    @Override
    public Long bGetLatestSequence(final byte[] seqKey) {
        return FutureHelper.get(getLatestSequence(seqKey), this.futureTimeoutMillis);
    }

    @Override
    public Long bGetLatestSequence(final long regionId, final byte[] seqKey) {
        return FutureHelper.get(getLatestSequence(regionId, seqKey), this.futureTimeoutMillis);
    }

    @Override
    public Long bGetLatestSequence(final String seqKey) {
        return FutureHelper.get(getLatestSequence(seqKey), this.futureTimeoutMillis);
    }

    @Override
    public Long bGetLatestSequence(final long regionId, final String seqKey) {
        return FutureHelper.get(getLatestSequence(regionId, seqKey), this.futureTimeoutMillis);
    }

    private void internalGetSequence(final byte[] seqKey, final int step, final CompletableFuture<Sequence> future,
                                     final int retriesLeft, final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(seqKey, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalGetSequence(seqKey, step, future,
                retriesLeft - 1, retryCause, regionId);
        final FailoverClosure<Sequence> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).getSequence(seqKey, step, closure);
            }
        } else {
            final GetSequenceRequest request = new GetSequenceRequest();
            request.setSeqKey(seqKey);
            request.setStep(step);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> resetSequence(final byte[] seqKey) {
        return resetSequence(ANY_REGION_ID, seqKey);
    }

    @Override
    public CompletableFuture<Boolean> resetSequence(final long regionId, final byte[] seqKey) {
        checkState();
        Requires.requireNonNull(seqKey, "seqKey");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalResetSequence(seqKey, future, this.failoverRetries, null, regionId);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> resetSequence(final String seqKey) {
        return resetSequence(BytesUtil.writeUtf8(seqKey));
    }

    @Override
    public CompletableFuture<Boolean> resetSequence(final long regionId, final String seqKey) {
        return resetSequence(regionId, BytesUtil.writeUtf8(seqKey));
    }

    @Override
    public Boolean bResetSequence(final byte[] seqKey) {
        return FutureHelper.get(resetSequence(seqKey), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bResetSequence(final long regionId, final byte[] seqKey) {
        return FutureHelper.get(resetSequence(regionId, seqKey), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bResetSequence(final String seqKey) {
        return FutureHelper.get(resetSequence(seqKey), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bResetSequence(final long regionId, final String seqKey) {
        return FutureHelper.get(resetSequence(regionId, seqKey), this.futureTimeoutMillis);
    }

    private void internalResetSequence(final byte[] seqKey, final CompletableFuture<Boolean> future,
                                       final int retriesLeft, final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(seqKey, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalResetSequence(seqKey, future, retriesLeft - 1,
                retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).resetSequence(seqKey, closure);
            }
        } else {
            final ResetSequenceRequest request = new ResetSequenceRequest();
            request.setSeqKey(seqKey);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> put(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return put(key, value, new CompletableFuture<>(), true, ANY_REGION_ID);
    }

    @Override
    public CompletableFuture<Boolean> put(final long regionId, final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return put(key, value, new CompletableFuture<>(), true, regionId);
    }

    @Override
    public CompletableFuture<Boolean> put(final String key, final byte[] value) {
        return put(BytesUtil.writeUtf8(key), value);
    }

    @Override
    public CompletableFuture<Boolean> put(final long regionId, final String key, final byte[] value) {
        return put(regionId, BytesUtil.writeUtf8(key), value);
    }

    @Override
    public Boolean bPut(final byte[] key, final byte[] value) {
        return FutureHelper.get(put(key, value), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bPut(final long regionId, final byte[] key, final byte[] value) {
        return FutureHelper.get(put(regionId, key, value), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bPut(final String key, final byte[] value) {
        return FutureHelper.get(put(key, value), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bPut(final long regionId, final String key, final byte[] value) {
        return FutureHelper.get(put(regionId, key, value), this.futureTimeoutMillis);
    }

    private CompletableFuture<Boolean> put(final byte[] key, final byte[] value,
                                           final CompletableFuture<Boolean> future, final boolean tryBatching,
                                           final long regionId) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        if (tryBatching) {
            final PutBatching putBatching = this.putBatching;
            if (putBatching != null && putBatching.apply(regionId, new KVEntry(regionId, key, value), future)) {
                return future;
            }
        }
        internalPut(key, value, future, this.failoverRetries, null, regionId);
        return future;
    }

    private void internalPut(final byte[] key, final byte[] value, final CompletableFuture<Boolean> future,
                             final int retriesLeft, final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalPut(key, value, future, retriesLeft - 1,
                retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).put(key, value, closure);
            }
        } else {
            final PutRequest request = new PutRequest();
            request.setKey(key);
            request.setValue(value);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<byte[]> getAndPut(final byte[] key, final byte[] value) {
        return getAndPut(ANY_REGION_ID, key, value);
    }

    @Override
    public CompletableFuture<byte[]> getAndPut(final long regionId, final byte[] key, final byte[] value) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        internalGetAndPut(key, value, future, this.failoverRetries, null, regionId);
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getAndPut(final String key, final byte[] value) {
        return getAndPut(BytesUtil.writeUtf8(key), value);
    }

    @Override
    public CompletableFuture<byte[]> getAndPut(final long regionId, final String key, final byte[] value) {
        return getAndPut(regionId, BytesUtil.writeUtf8(key), value);
    }

    @Override
    public byte[] bGetAndPut(final byte[] key, final byte[] value) {
        return FutureHelper.get(getAndPut(key, value), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGetAndPut(final long regionId, final byte[] key, final byte[] value) {
        return FutureHelper.get(getAndPut(regionId, key, value), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGetAndPut(final String key, final byte[] value) {
        return FutureHelper.get(getAndPut(key, value), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bGetAndPut(final long regionId, final String key, final byte[] value) {
        return FutureHelper.get(getAndPut(regionId, key, value), this.futureTimeoutMillis);
    }

    private void internalGetAndPut(final byte[] key, final byte[] value, final CompletableFuture<byte[]> future,
                                   final int retriesLeft, final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalGetAndPut(key, value, future, retriesLeft - 1,
                retryCause, regionId);
        final FailoverClosure<byte[]> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).getAndPut(key, value, closure);
            }
        } else {
            final GetAndPutRequest request = new GetAndPutRequest();
            request.setKey(key);
            request.setValue(value);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> compareAndPut(final byte[] key, final byte[] expect, final byte[] update) {
        return compareAndPut(ANY_REGION_ID, key, expect, update);
    }

    @Override
    public CompletableFuture<Boolean> compareAndPut(final long regionId, final byte[] key, final byte[] expect,
                                                    final byte[] update) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(expect, "expect");
        Requires.requireNonNull(update, "update");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalCompareAndPut(key, expect, update, future, this.failoverRetries, null, regionId);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> compareAndPut(final String key, final byte[] expect, final byte[] update) {
        return compareAndPut(BytesUtil.writeUtf8(key), expect, update);
    }

    @Override
    public CompletableFuture<Boolean> compareAndPut(final long regionId, final String key, final byte[] expect,
                                                    final byte[] update) {
        return compareAndPut(regionId, BytesUtil.writeUtf8(key), expect, update);
    }

    @Override
    public Boolean bCompareAndPut(final byte[] key, final byte[] expect, final byte[] update) {
        return FutureHelper.get(compareAndPut(key, expect, update), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bCompareAndPut(final long regionId, final byte[] key, final byte[] expect, final byte[] update) {
        return FutureHelper.get(compareAndPut(regionId, key, expect, update), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bCompareAndPut(final String key, final byte[] expect, final byte[] update) {
        return FutureHelper.get(compareAndPut(key, expect, update), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bCompareAndPut(final long regionId, final String key, final byte[] expect, final byte[] update) {
        return FutureHelper.get(compareAndPut(regionId, key, expect, update), this.futureTimeoutMillis);
    }

    private void internalCompareAndPut(final byte[] key, final byte[] expect, final byte[] update,
                                       final CompletableFuture<Boolean> future, final int retriesLeft,
                                       final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalCompareAndPut(key, expect, update, future, retriesLeft - 1,
                retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).compareAndPut(key, expect, update, closure);
            }
        } else {
            final CompareAndPutRequest request = new CompareAndPutRequest();
            request.setKey(key);
            request.setExpect(expect);
            request.setUpdate(update);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> merge(final String key, final String value) {
        return merge(ANY_REGION_ID, key, value);
    }

    @Override
    public CompletableFuture<Boolean> merge(final long regionId, final String key, final String value) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalMerge(BytesUtil.writeUtf8(key), BytesUtil.writeUtf8(value), future, this.failoverRetries, null,
            regionId);
        return future;
    }

    @Override
    public Boolean bMerge(final String key, final String value) {
        return FutureHelper.get(merge(key, value), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bMerge(final long regionId, final String key, final String value) {
        return FutureHelper.get(merge(regionId, key, value), this.futureTimeoutMillis);
    }

    private void internalMerge(final byte[] key, final byte[] value, final CompletableFuture<Boolean> future,
                               final int retriesLeft, final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalMerge(key, value, future, retriesLeft - 1,
                retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).merge(key, value, closure);
            }
        } else {
            final MergeRequest request = new MergeRequest();
            request.setKey(key);
            request.setValue(value);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    // Note: the current implementation, if the 'keys' are distributed across
    // multiple regions, can not provide transaction guarantee.
    @Override
    public CompletableFuture<Boolean> put(final List<KVEntry> entries) {
        checkState();
        Requires.requireNonNull(entries, "entries");
        Requires.requireTrue(!entries.isEmpty(), "entries empty");
        final FutureGroup<Boolean> futureGroup = internalPut(entries, this.failoverRetries, null);
        return FutureHelper.joinBooleans(futureGroup);
    }

    @Override
    public Boolean bPut(final List<KVEntry> entries) {
        return FutureHelper.get(put(entries), this.futureTimeoutMillis);
    }

    private FutureGroup<Boolean> internalPut(final List<KVEntry> entries, final int retriesLeft,
                                             final Throwable lastCause) {
        final Map<Region, List<KVEntry>> regionMap = this.pdClient
                .findRegionsByKvEntries(entries, ApiExceptionHelper.isInvalidEpoch(lastCause));
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(regionMap.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Map.Entry<Region, List<KVEntry>> entry : regionMap.entrySet()) {
            final Region region = entry.getKey();
            final List<KVEntry> subEntries = entry.getValue();
            final RetryCallable<Boolean> retryCallable = retryCause -> internalPut(subEntries, retriesLeft - 1,
                    retryCause);
            final BoolFailoverFuture future = new BoolFailoverFuture(retriesLeft, retryCallable);
            internalRegionPut(region, subEntries, future, retriesLeft, lastError);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    private void internalRegionPut(final Region region, final List<KVEntry> subEntries,
                                   final CompletableFuture<Boolean> future, final int retriesLeft,
                                   final Errors lastCause) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionPut(region, subEntries, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (this.kvDispatcher == null) {
                    rawKVStore.put(subEntries, closure);
                } else {
                    this.kvDispatcher.execute(() -> rawKVStore.put(subEntries, closure));
                }
            }
        } else {
            final BatchPutRequest request = new BatchPutRequest();
            request.setKvEntries(subEntries);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<byte[]> putIfAbsent(final byte[] key, final byte[] value) {
        return putIfAbsent(ANY_REGION_ID, key, value);
    }

    @Override
    public CompletableFuture<byte[]> putIfAbsent(final long regionId, final byte[] key, final byte[] value) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        internalPutIfAbsent(key, value, future, this.failoverRetries, null, regionId);
        return future;
    }

    @Override
    public CompletableFuture<byte[]> putIfAbsent(final String key, final byte[] value) {
        return putIfAbsent(BytesUtil.writeUtf8(key), value);
    }

    @Override
    public CompletableFuture<byte[]> putIfAbsent(final long regionId, final String key, final byte[] value) {
        return putIfAbsent(regionId, BytesUtil.writeUtf8(key), value);
    }

    @Override
    public byte[] bPutIfAbsent(final byte[] key, final byte[] value) {
        return FutureHelper.get(putIfAbsent(key, value), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bPutIfAbsent(final long regionId, final byte[] key, final byte[] value) {
        return FutureHelper.get(putIfAbsent(regionId, key, value), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bPutIfAbsent(final String key, final byte[] value) {
        return FutureHelper.get(putIfAbsent(key, value), this.futureTimeoutMillis);
    }

    @Override
    public byte[] bPutIfAbsent(final long regionId, final String key, final byte[] value) {
        return FutureHelper.get(putIfAbsent(regionId, key, value), this.futureTimeoutMillis);
    }

    private void internalPutIfAbsent(final byte[] key, final byte[] value, final CompletableFuture<byte[]> future,
                                     final int retriesLeft, final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalPutIfAbsent(key, value, future, retriesLeft - 1,
                retryCause, regionId);
        final FailoverClosure<byte[]> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).putIfAbsent(key, value, closure);
            }
        } else {
            final PutIfAbsentRequest request = new PutIfAbsentRequest();
            request.setKey(key);
            request.setValue(value);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> delete(final byte[] key) {
        return delete(ANY_REGION_ID, key);
    }

    @Override
    public CompletableFuture<Boolean> delete(final long regionId, final byte[] key) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(key, "key");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalDelete(key, future, this.failoverRetries, null, regionId);
        return future;
    }

    @Override
    public CompletableFuture<Boolean> delete(final String key) {
        return delete(BytesUtil.writeUtf8(key));
    }

    @Override
    public CompletableFuture<Boolean> delete(final long regionId, final String key) {
        return delete(regionId, BytesUtil.writeUtf8(key));
    }

    @Override
    public Boolean bDelete(final byte[] key) {
        return FutureHelper.get(delete(key), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDelete(final long regionId, final byte[] key) {
        return FutureHelper.get(delete(regionId, key), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDelete(final String key) {
        return FutureHelper.get(delete(key), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDelete(final long regionId, final String key) {
        return FutureHelper.get(delete(regionId, key), this.futureTimeoutMillis);
    }

    private void internalDelete(final byte[] key, final CompletableFuture<Boolean> future, final int retriesLeft,
                                final Errors lastCause, final long regionId) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause)) :
                this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalDelete(key, future, retriesLeft - 1, retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).delete(key, closure);
            }
        } else {
            final DeleteRequest request = new DeleteRequest();
            request.setKey(key);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> deleteRange(final byte[] startKey, final byte[] endKey) {
        return deleteRange(ANY_REGION_ID, startKey, endKey);
    }

    @Override
    public CompletableFuture<Boolean> deleteRange(final long regionId, final byte[] startKey, final byte[] endKey) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(startKey, "startKey");
        Requires.requireNonNull(endKey, "endKey");
        Requires.requireTrue(BytesUtil.compare(startKey, endKey) < 0, "startKey must < endKey");
        final FutureGroup<Boolean> futureGroup = internalDeleteRange(startKey, endKey, this.failoverRetries, null,
            regionId);
        return FutureHelper.joinBooleans(futureGroup);
    }

    private FutureGroup<Boolean> internalDeleteRange(final byte[] startKey, final byte[] endKey, final int retriesLeft,
                                                     final Throwable lastCause, final long regionId) {
        final List<Region> regionList = (regionId == ANY_REGION_ID) ?
                this.pdClient.findRegionsByKeyRange(startKey, endKey, ApiExceptionHelper.isInvalidEpoch(lastCause)) :
                Lists.newArrayList(this.pdClient.getRegionById(regionId));
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(regionList.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Region region : regionList) {
            final byte[] regionStartKey = region.getStartKey();
            final byte[] regionEndKey = region.getEndKey();
            final byte[] subStartKey = regionStartKey == null ? startKey : BytesUtil.max(regionStartKey, startKey);
            final byte[] subEndKey = regionEndKey == null ? endKey : BytesUtil.min(regionEndKey, endKey);
            final RetryCallable<Boolean> retryCallable = retryCause -> internalDeleteRange(subStartKey, subEndKey,
                    retriesLeft - 1, retryCause, regionId);
            final BoolFailoverFuture future = new BoolFailoverFuture(retriesLeft, retryCallable);
            internalRegionDeleteRange(region, subStartKey, subEndKey, future, retriesLeft, lastError);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    @Override
    public CompletableFuture<Boolean> deleteRange(final String startKey, final String endKey) {
        return deleteRange(BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey));
    }

    @Override
    public CompletableFuture<Boolean> deleteRange(final long regionId, final String startKey, final String endKey) {
        return deleteRange(regionId, BytesUtil.writeUtf8(startKey), BytesUtil.writeUtf8(endKey));
    }

    @Override
    public Boolean bDeleteRange(final byte[] startKey, final byte[] endKey) {
        return FutureHelper.get(deleteRange(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDeleteRange(final long regionId, final byte[] startKey, final byte[] endKey) {
        return FutureHelper.get(deleteRange(regionId, startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDeleteRange(final String startKey, final String endKey) {
        return FutureHelper.get(deleteRange(startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDeleteRange(final long regionId, final String startKey, final String endKey) {
        return FutureHelper.get(deleteRange(regionId, startKey, endKey), this.futureTimeoutMillis);
    }

    @Override
    public CompletableFuture<Boolean> delete(final List<byte[]> keys) {
        return delete(ANY_REGION_ID, keys);
    }

    @Override
    public CompletableFuture<Boolean> delete(final long regionId, final List<byte[]> keys) {
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);
        Requires.requireNonNull(keys, "keys");
        Requires.requireTrue(!keys.isEmpty(), "keys empty");
        final FutureGroup<Boolean> futureGroup = internalDelete(keys, this.failoverRetries, null, regionId);
        return FutureHelper.joinBooleans(futureGroup);
    }

    @Override
    public Boolean bDelete(final List<byte[]> keys) {
        return FutureHelper.get(delete(keys), this.futureTimeoutMillis);
    }

    @Override
    public Boolean bDelete(final long regionId, final List<byte[]> keys) {
        return FutureHelper.get(delete(regionId, keys), this.futureTimeoutMillis);
    }

    private FutureGroup<Boolean> internalDelete(final List<byte[]> keys, final int retriesLeft,
                                                final Throwable lastCause, final long regionId) {
        Map<Region, List<byte[]>> regionMap;
        if (regionId == ANY_REGION_ID) {
            regionMap = this.pdClient.findRegionsByKeys(keys, ApiExceptionHelper.isInvalidEpoch(lastCause));
        } else {
            regionMap = Maps.newHashMap();
            regionMap.put(this.pdClient.getRegionById(regionId), keys);
        }
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(regionMap.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Map.Entry<Region, List<byte[]>> entry : regionMap.entrySet()) {
            final Region region = entry.getKey();
            final List<byte[]> subKeys = entry.getValue();
            final RetryCallable<Boolean> retryCallable = retryCause -> internalDelete(subKeys, retriesLeft - 1,
                    retryCause, regionId);
            final BoolFailoverFuture future = new BoolFailoverFuture(retriesLeft, retryCallable);
            internalRegionDelete(region, subKeys, future, retriesLeft, lastError);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    private void internalRegionDelete(final Region region, final List<byte[]> subKeys,
                                      final CompletableFuture<Boolean> future, final int retriesLeft,
                                      final Errors lastCause) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionDelete(region, subKeys, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (this.kvDispatcher == null) {
                    rawKVStore.delete(subKeys, closure);
                } else {
                    this.kvDispatcher.execute(() -> rawKVStore.delete(subKeys, closure));
                }
            }
        } else {
            final BatchDeleteRequest request = new BatchDeleteRequest();
            request.setKeys(subKeys);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    private void internalRegionDeleteRange(final Region region, final byte[] subStartKey, final byte[] subEndKey,
                                           final CompletableFuture<Boolean> future, final int retriesLeft,
                                           final Errors lastCause) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionDeleteRange(region, subStartKey, subEndKey, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure =
                new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).deleteRange(subStartKey, subEndKey, closure);
            }
        } else {
            final DeleteRangeRequest request = new DeleteRangeRequest();
            request.setStartKey(subStartKey);
            request.setEndKey(subEndKey);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public Boolean bBatch(final List<KVOperation> kvOperations) {
        return FutureHelper.get(batch(kvOperations), this.futureTimeoutMillis);
    }

    // batch operation with composite put or delete
    // Note: the current implementation, if the 'keys' are distributed across
    // multiple regions, can not provide transaction guarantee.
    @Override
    public CompletableFuture<Boolean> batch(final List<KVOperation> kvOperations) {
        checkState();
        Requires.requireNonNull(kvOperations, "kvOperations");
        Requires.requireTrue(!kvOperations.isEmpty(), "kvOperations empty");
        final FutureGroup<Boolean> futureGroup = internalBatch(kvOperations, this.failoverRetries, null);
        return FutureHelper.joinBooleans(futureGroup);
    }

    private FutureGroup<Boolean> internalBatch(final List<KVOperation> kvCompositeOperations, final int retriesLeft,
                                                final Throwable lastCause) {
        final Map<Region, List<KVOperation>> regionMap = this.pdClient
                .findRegionsByKvOperations(kvCompositeOperations, ApiExceptionHelper.isInvalidEpoch(lastCause));
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(regionMap.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Map.Entry<Region, List<KVOperation>> entry : regionMap.entrySet()) {
            final Region region = entry.getKey();
            final List<KVOperation> subKVOperations = entry.getValue();
            final RetryCallable<Boolean> retryCallable = retryCause -> internalBatch(subKVOperations, retriesLeft - 1,
                    retryCause);
            final BoolFailoverFuture future = new BoolFailoverFuture(retriesLeft, retryCallable);
            internalRegionBatch(region, subKVOperations, future, retriesLeft, lastError);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    private void internalRegionBatch(final Region region, final List<KVOperation> subKVOperations,
                                   final CompletableFuture<Boolean> future, final int retriesLeft,
                                   final Errors lastCause) {
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionBatch(region, subKVOperations, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final RawKVStore rawKVStore = getRawKVStore(regionEngine);
                if (this.kvDispatcher == null) {
                    rawKVStore.batch(subKVOperations, closure);
                } else {
                    this.kvDispatcher.execute(() -> rawKVStore.batch(subKVOperations, closure));
                }
            }
        } else {
            final BatchCompositeRequest request = new BatchCompositeRequest();
            request.setCompositeRequests(createCompositeRequests(subKVOperations, region.getId(), region.getRegionEpoch()));
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    private List<BaseRequest> createCompositeRequests(final List<KVOperation> kvOperations, final long regionId,
                                                      final RegionEpoch regionEpoch) {
        List<BaseRequest> requests = new LinkedList<>();
        for (KVOperation op : kvOperations) {
            switch (op.getOp()) {
                case KVOperation.PUT:
                    requests.add(new PutRequest(op.getKey(), op.getValue(), regionId, regionEpoch));
                    break;
                case KVOperation.PUT_IF_ABSENT:
                    requests.add(new PutIfAbsentRequest(op.getKey(), op.getValue(), regionId, regionEpoch));
                    break;
                case KVOperation.DELETE:
                    requests.add(new DeleteRequest(op.getKey(), regionId, regionEpoch));
                    break;
                case KVOperation.PUT_LIST:
                    requests.add(new BatchPutRequest(op.getEntries(), regionId, regionEpoch));
                    break;
                case KVOperation.DELETE_RANGE:
                    requests.add(new DeleteRangeRequest(op.getStartKey(), op.getEndKey(), regionId, regionEpoch));
                    break;
                case KVOperation.GET_SEQUENCE:
                    requests.add(new GetSequenceRequest(op.getSeqKey(), op.getStep(), regionId, regionEpoch));
                    break;
                case KVOperation.NODE_EXECUTE:
                    requests.add(new NodeExecuteRequest(op.getNodeExecutor(), regionId, regionEpoch));
                    break;
                case KVOperation.KEY_LOCK:
                    requests.add(new KeyLockRequest(op.getKey(), true, op.getAcquirer(), regionId, regionEpoch));
                    break;
                case KVOperation.KEY_LOCK_RELEASE:
                    requests.add(new KeyUnlockRequest(op.getKey(), op.getAcquirer(), regionId, regionEpoch));
                    break;
                case KVOperation.GET:
                    requests.add(new GetRequest(op.getKey(), regionId, regionEpoch));
                    break;
                case KVOperation.MULTI_GET:
                    requests.add(new MultiGetRequest(op.getKeys(), regionId, regionEpoch));
                    break;
                case KVOperation.SCAN:
                    requests
                        .add(new ScanRequest(op.getStartKey(), op.getEndKey(), op.getLimit(), regionId, regionEpoch));
                    break;
                case KVOperation.GET_PUT:
                    requests.add(new GetAndPutRequest(op.getKey(), op.getValue(), regionId, regionEpoch));
                    break;
                case KVOperation.MERGE:
                    requests.add(new MergeRequest(op.getKey(), op.getValue(), regionId, regionEpoch));
                    break;
                case KVOperation.RESET_SEQUENCE:
                    requests.add(new ResetSequenceRequest(op.getSeqKey(), regionId, regionEpoch));
                    break;
                case KVOperation.RANGE_SPLIT:
                    requests.add(new RangeSplitRequest(op.getNewRegionId(), regionId, regionEpoch));
                    break;
                case KVOperation.COMPARE_PUT:
                    requests.add(new CompareAndPutRequest(op.getKey(), op.getExpect(), op.getValue(), regionId,
                        regionEpoch));
                    break;
                case KVOperation.DELETE_LIST:
                    requests.add(new BatchDeleteRequest(op.getKeys(), regionId, regionEpoch));
                    break;
                case KVOperation.CONTAINS_KEY:
                    requests.add(new ContainsKeyRequest(op.getKey(), regionId, regionEpoch));
                    break;
                default:
                    throw new UnsupportedOperationException("batch op " + op.getOp() + " is not supported yet");
            }
        }
        return requests;
    }

    // internal api
    public CompletableFuture<Boolean> execute(final long regionId, final NodeExecutor executor) {
        checkState();
        Requires.requireNonNull(executor, "executor");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalExecute(regionId, executor, future, this.failoverRetries, null);
        return future;
    }

    // internal api
    public Boolean bExecute(final long regionId, final NodeExecutor executor) {
        return FutureHelper.get(execute(regionId, executor), this.futureTimeoutMillis);
    }

    private void internalExecute(final long regionId, final NodeExecutor executor,
                                 final CompletableFuture<Boolean> future, final int retriesLeft, final Errors lastCause) {
        final Region region = this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalExecute(regionId, executor, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).execute(executor, true, closure);
            }
        } else {
            final NodeExecuteRequest request = new NodeExecuteRequest();
            request.setNodeExecutor(executor);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit) {
        return getDistributedLock(target, lease, unit, null);
    }

    @Override
    public DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit) {
        return getDistributedLock(target, lease, unit, null);
    }

    @Override
    public DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit,
                                                      final ScheduledExecutorService watchdog) {
        return new DefaultDistributedLock(target, lease, unit, watchdog, this);
    }

    @Override
    public DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit,
                                                      final ScheduledExecutorService watchdog) {
        return getDistributedLock(BytesUtil.writeUtf8(target), lease, unit, watchdog);
    }

    public CompletableFuture<DistributedLock.Owner> tryLockWith(final byte[] key, final boolean keepLease,
                                                                final DistributedLock.Acquirer acquirer) {
        checkState();
        Requires.requireNonNull(key, "key");
        final CompletableFuture<DistributedLock.Owner> future = new CompletableFuture<>();
        internalTryLockWith(key, keepLease, acquirer, future, this.failoverRetries, null);
        return future;
    }

    private void internalTryLockWith(final byte[] key, final boolean keepLease, final DistributedLock.Acquirer acquirer,
                                     final CompletableFuture<DistributedLock.Owner> future, final int retriesLeft,
                                     final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalTryLockWith(key, keepLease, acquirer, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<DistributedLock.Owner> closure = new FailoverClosureImpl<>(future, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).tryLockWith(key, region.getStartKey(), keepLease, acquirer, closure);
            }
        } else {
            final KeyLockRequest request = new KeyLockRequest();
            request.setKey(key);
            request.setKeepLease(keepLease);
            request.setAcquirer(acquirer);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    public CompletableFuture<DistributedLock.Owner> releaseLockWith(final byte[] key,
                                                                    final DistributedLock.Acquirer acquirer) {
        checkState();
        Requires.requireNonNull(key, "key");
        final CompletableFuture<DistributedLock.Owner> future = new CompletableFuture<>();
        internalReleaseLockWith(key, acquirer, future, this.failoverRetries, null);
        return future;
    }

    private void internalReleaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer,
                                         final CompletableFuture<DistributedLock.Owner> future, final int retriesLeft,
                                         final Errors lastCause) {
        final Region region = this.pdClient.findRegionByKey(key, ErrorsHelper.isInvalidEpoch(lastCause));
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalReleaseLockWith(key, acquirer, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<DistributedLock.Owner> closure = new FailoverClosureImpl<>(future, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).releaseLockWith(key, acquirer, closure);
            }
        } else {
            final KeyUnlockRequest request = new KeyUnlockRequest();
            request.setKey(key);
            request.setAcquirer(acquirer);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public PlacementDriverClient getPlacementDriverClient() {
        return pdClient;
    }

    @Override
    public void addLeaderStateListener(final long regionId, final LeaderStateListener listener) {
        addStateListener(regionId, listener);
    }

    @Override
    public void addFollowerStateListener(final long regionId, final FollowerStateListener listener) {
        addStateListener(regionId, listener);
    }

    @Override
    public void addStateListener(final long regionId, final StateListener listener) {
        this.stateListenerContainer.addStateListener(regionId, listener);
    }

    @Override
    public CompletableFuture<Boolean> destroyRegion(final long regionId) {
        if (regionId == ANY_REGION_ID) {
            throw new UnsupportedOperationException("destroyRegion without regionId specified");
        }
        checkState();
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalDestroyRegion(future, this.failoverRetries, null, regionId);
        return future;
    }

    private void internalDestroyRegion(final CompletableFuture<Boolean> future, final int retriesLeft,
                                         final Errors lastCause, final long regionId) {
        final Region region = this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalDestroyRegion(future,
                retriesLeft - 1, retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, true, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).destroy(regionId, closure);
            }
        } else {
            final DestroyRegionRequest request = new DestroyRegionRequest();
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> sealRegion(final long regionId) {
        if (regionId == ANY_REGION_ID) {
            throw new UnsupportedOperationException("sealRegion without regionId specified");
        }
        checkState();
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalSealRegion(future, this.failoverRetries, null, regionId);
        return future;
    }

    private void internalSealRegion(final CompletableFuture<Boolean> future, final int retriesLeft,
                                       final Errors lastCause, final long regionId) {
        final Region region = this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalSealRegion(future,
                retriesLeft - 1, retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).seal(regionId, closure);
            }
        } else {
            final SealRegionRequest request = new SealRegionRequest();
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public CompletableFuture<Boolean> isRegionSealed(final long regionId) {
        checkState();
        if (regionId == ANY_REGION_ID) {
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();
            for (Long id : this.pdClient.getRegionIds()) {
                final CompletableFuture<Boolean> future = new CompletableFuture<>();
                futures.add(future);
                internalIsRegionSealed(future, this.failoverRetries, null, id);
            }
            FutureGroup<Boolean> futureGroup = new FutureGroup<>(futures);
            return CompletableFuture.allOf(futureGroup.toArray()).thenApply(
                    v -> futures.stream().allMatch(CompletableFuture::join));
        }
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalIsRegionSealed(future, this.failoverRetries, null, regionId);
        return future;
    }

    private void internalIsRegionSealed(final CompletableFuture<Boolean> future, final int retriesLeft,
                                    final Errors lastCause, final long regionId) {
        final Region region = this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalIsRegionSealed(future,
                retriesLeft - 1, retryCause, regionId);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).isSealed(regionId, closure);
            }
        } else {
            final IsRegionSealedRequest request = new IsRegionSealedRequest();
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    @Override
    public Boolean bIsRegionSealed(final long regionId) {
        return FutureHelper.get(isRegionSealed(regionId), this.futureTimeoutMillis);
    }

    @Override
    public CompletableFuture<Long> size(final long regionId) {
        checkState();
        if (regionId == ANY_REGION_ID) {
            return size();
        }
        final CompletableFuture<Long> future = new CompletableFuture<>();
        internalGetSizeSingleRegion(future, this.failoverRetries, null, regionId);
        return future;
    }

    @Override
    public Long bSize(final long regionId) {
        return FutureHelper.get(size(regionId), this.futureTimeoutMillis);
    }

    @Override
    public CompletableFuture<Long> size() {
        checkState();
        final List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (long regionId : this.pdClient.getRegionIds()) {
            final CompletableFuture<Long> future = new CompletableFuture<>();
            internalGetSizeSingleRegion(future, this.failoverRetries, null, regionId);
            futures.add(future);
        }
        FutureGroup<Long> futureGroup = new FutureGroup<>(futures);
        return CompletableFuture.allOf(futureGroup.toArray()).thenApply(
                v -> futures.stream().mapToLong(CompletableFuture::join).sum());
    }

    @Override
    public Long bSize() {
        return FutureHelper.get(size(), this.futureTimeoutMillis);
    }

    private void internalGetSizeSingleRegion(final CompletableFuture<Long> future, final int retriesLeft,
                                    final Errors lastCause, final long regionId) {
        final Region region = this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalGetSizeSingleRegion(future,
                retriesLeft - 1, retryCause, regionId);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                getRawKVStore(regionEngine).size(closure);
            }
        } else {
            final GetSizeRequest request = new GetSizeRequest();
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            this.rheaKVRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    public long getClusterId() {
        return this.opts.getClusterId();
    }

    public StoreEngine getStoreEngine() {
        return storeEngine;
    }

    public boolean isOnlyLeaderRead() {
        return onlyLeaderRead;
    }

    public boolean isLeader(final long regionId) {
        checkState();
        final RegionEngine regionEngine = getRegionEngine(regionId);
        return regionEngine != null && regionEngine.isLeader();
    }

    private void checkState() {
        // Not a strict state check, more is to use a read volatile operation to make
        // a happen-before, because the init() method finally wrote 'this.started'
        if (!this.started) {
            throw new RheaRuntimeException("rhea kv is not started or shutdown");
        }
    }

    private RegionEngine getRegionEngine(final long regionId) {
        if (this.storeEngine == null) {
            return null;
        }
        return this.storeEngine.getRegionEngine(regionId);
    }

    private RegionEngine getRegionEngine(final long regionId, final boolean requireLeader) {
        final RegionEngine engine = getRegionEngine(regionId);
        if (engine == null) {
            return null;
        }
        if (requireLeader && !engine.isLeader()) {
            return null;
        }
        return engine;
    }

    private Endpoint getLeaderByRegionEngine(final long regionId) {
        final RegionEngine regionEngine = getRegionEngine(regionId);
        if (regionEngine != null) {
            final PeerId leader = regionEngine.getLeaderId();
            if (leader != null) {
                final String raftGroupId = JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), regionId);
                RouteTable.getInstance().updateLeader(raftGroupId, leader);
                return leader.getEndpoint();
            }
        }
        return null;
    }

    private RawKVStore getRawKVStore(final RegionEngine engine) {
        return engine.getMetricsRawKVStore();
    }

    private static boolean ensureOnValidEpoch(final Region region, final RegionEngine engine,
                                              final KVStoreClosure closure) {
        if (isValidEpoch(region, engine)) {
            return true;
        }
        // will retry on this error and status
        closure.setError(Errors.INVALID_REGION_EPOCH);
        closure.run(new Status(-1, "Invalid region epoch: %s", region));
        return false;
    }

    private static boolean isValidEpoch(final Region region, final RegionEngine engine) {
        return region.getRegionEpoch().equals(engine.getRegion().getRegionEpoch());
    }

    private class GetBatching extends Batching<KeyEvent, byte[], byte[]> {

        public GetBatching(EventFactory<KeyEvent> factory, String name, EventHandler<KeyEvent> handler) {
            super(factory, batchingOpts.getBufSize(), name, handler);
        }

        @Override
        public boolean apply(final long regionId, final byte[] message, final CompletableFuture<byte[]> future) {
            return this.ringBuffer.tryPublishEvent((event, sequence) -> {
                event.reset();
                event.regionId = regionId;
                event.key = message;
                event.future = future;
            });
        }
    }

    private class PutBatching extends Batching<KVEvent, KVEntry, Boolean> {

        public PutBatching(EventFactory<KVEvent> factory, String name, PutBatchingHandler handler) {
            super(factory, batchingOpts.getBufSize(), name, handler);
        }

        @Override
        public boolean apply(final long regionId, final KVEntry message, final CompletableFuture<Boolean> future) {
            return this.ringBuffer.tryPublishEvent((event, sequence) -> {
                event.reset();
                event.regionId = regionId;
                event.kvEntry = message;
                event.future = future;
            });
        }
    }

    private class CompositeBatching extends Batching<KVCompositeEvent, KVOperation, Boolean> {

        public CompositeBatching(EventFactory<KVCompositeEvent> factory, String name, CompositeBatchingHandler handler) {
            super(factory, batchingOpts.getBufSize(), name, handler);
        }

        @Override
        public boolean apply(final long regionId, final KVOperation message, final CompletableFuture<Boolean> future) {
            return this.ringBuffer.tryPublishEvent((event, sequence) -> {
                event.reset();
                event.regionId = regionId;
                event.kvOperation = message;
                event.future = future;
            });
        }
    }

    private class GetBatchingHandler extends AbstractBatchingHandler<KeyEvent> {

        private final boolean readOnlySafe;

        private GetBatchingHandler(String metricsName, boolean readOnlySafe) {
            super(metricsName);
            this.readOnlySafe = readOnlySafe;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(final KeyEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            this.events.add(event);
            this.cachedBytes += event.key.length;
            final int size = this.events.size();
            if (!endOfBatch && size < batchingOpts.getBatchSize() && this.cachedBytes < batchingOpts.getMaxReadBytes()) {
                return;
            }

            if (size == 1) {
                reset();
                try {
                    get(event.key, this.readOnlySafe, event.future, false, event.regionId);
                } catch (final Throwable t) {
                    exceptionally(t, event.future);
                }
            } else {
                final List<byte[]> keys = Lists.newArrayListWithCapacity(size);
                final CompletableFuture<byte[]>[] futures = new CompletableFuture[size];
                for (int i = 0; i < size; i++) {
                    final KeyEvent e = this.events.get(i);
                    keys.add(e.key);
                    futures[i] = e.future;
                }
                reset();
                try {
                    multiGet(event.regionId, keys, this.readOnlySafe).whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            for (int i = 0; i < futures.length; i++) {
                                final ByteArray realKey = ByteArray.wrap(keys.get(i));
                                futures[i].complete(result.get(realKey));
                            }
                            return;
                        }
                        exceptionally(throwable, futures);
                    });
                } catch (final Throwable t) {
                    exceptionally(t, futures);
                }
            }
        }
    }

    private class PutBatchingHandler extends AbstractBatchingHandler<KVEvent> {

        public PutBatchingHandler(String metricsName) {
            super(metricsName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(final KVEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            this.events.add(event);
            this.cachedBytes += event.kvEntry.length();
            final int size = this.events.size();
            if (!endOfBatch && size < batchingOpts.getBatchSize() && this.cachedBytes < batchingOpts.getMaxWriteBytes()) {
                return;
            }

            if (size == 1) {
                reset();
                final KVEntry kv = event.kvEntry;
                try {
                    put(kv.getKey(), kv.getValue(), event.future, false, kv.getRegionId());
                } catch (final Throwable t) {
                    exceptionally(t, event.future);
                }
            } else {
                final List<KVEntry> entries = Lists.newArrayListWithCapacity(size);
                final CompletableFuture<Boolean>[] futures = new CompletableFuture[size];
                for (int i = 0; i < size; i++) {
                    final KVEvent e = this.events.get(i);
                    entries.add(e.kvEntry);
                    futures[i] = e.future;
                }
                reset();
                try {
                    put(entries).whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            for (int i = 0; i < futures.length; i++) {
                                futures[i].complete(result);
                            }
                            return;
                        }
                        exceptionally(throwable, futures);
                    });
                } catch (final Throwable t) {
                    exceptionally(t, futures);
                }
            }
        }
    }

    private class CompositeBatchingHandler extends AbstractBatchingHandler<KVCompositeEvent> {

        public CompositeBatchingHandler(String metricsName) {
            super(metricsName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(final KVCompositeEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            this.events.add(event);
            this.cachedBytes += event.kvOperation.getValue().length;
            final int size = this.events.size();
            if (!endOfBatch && size < batchingOpts.getBatchSize() && this.cachedBytes < batchingOpts.getMaxWriteBytes()) {
                return;
            }

            if (size == 1) {
                reset();
                final KVOperation operation = event.kvOperation;
                try {
                    assert operation.getOp() == KVOperation.BATCH_OP;
                    batch(operation.getKVOperations()).whenComplete((r, t) -> {
                        if (t == null) {
                            event.future.complete(r);
                        } else {
                            event.future.completeExceptionally(t);
                        }
                    });
                } catch (final Throwable t) {
                    exceptionally(t, event.future);
                }
            } else {
                final List<KVOperation> entries = Lists.newArrayListWithCapacity(size);
                final CompletableFuture<Boolean>[] futures = new CompletableFuture[size];
                for (int i = 0; i < size; i++) {
                    final KVCompositeEvent e = this.events.get(i);
                    entries.add(e.kvOperation);
                    futures[i] = e.future;
                }
                reset();
                try {
                    batch(entries).whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            for (int i = 0; i < futures.length; i++) {
                                futures[i].complete(result);
                            }
                            return;
                        }
                        exceptionally(throwable, futures);
                    });
                } catch (final Throwable t) {
                    exceptionally(t, futures);
                }
            }
        }
    }

    private abstract class AbstractBatchingHandler<T> implements EventHandler<T> {

        protected final Histogram histogramWithKeys;
        protected final Histogram histogramWithBytes;

        protected final List<T>   events      = Lists.newArrayListWithCapacity(batchingOpts.getBatchSize());
        protected int             cachedBytes = 0;

        public AbstractBatchingHandler(String metricsName) {
            this.histogramWithKeys = KVMetrics.histogram(KVMetricNames.SEND_BATCHING, metricsName + "_keys");
            this.histogramWithBytes = KVMetrics.histogram(KVMetricNames.SEND_BATCHING, metricsName + "_bytes");
        }

        public void exceptionally(final Throwable t, final CompletableFuture<?>... futures) {
            for (int i = 0; i < futures.length; i++) {
                futures[i].completeExceptionally(t);
            }
        }

        public void reset() {
            this.histogramWithKeys.update(this.events.size());
            this.histogramWithBytes.update(this.cachedBytes);

            this.events.clear();
            this.cachedBytes = 0;
        }
    }

    private static class KeyEvent {

        // regionId here is used for GET since key has no regionId info.
        private long                      regionId = ANY_REGION_ID;
        private byte[]                    key;
        private CompletableFuture<byte[]> future;

        public void reset() {
            this.regionId = ANY_REGION_ID;
            this.key = null;
            this.future = null;
        }
    }

    private static class KVEvent {

        // regionId here is never used.
        @Deprecated
        private long                       regionId = ANY_REGION_ID;
        private KVEntry                    kvEntry;
        private CompletableFuture<Boolean> future;

        public void reset() {
            this.regionId = ANY_REGION_ID;
            this.kvEntry = null;
            this.future = null;
        }
    }

    private static class KVCompositeEvent {

        // regionId here is never used.
        @Deprecated
        private long                       regionId = ANY_REGION_ID;
        private KVOperation                kvOperation;
        private CompletableFuture<Boolean> future;

        public void reset() {
            this.regionId = ANY_REGION_ID;
            this.kvOperation = null;
            this.future = null;
        }
    }

    private static abstract class Batching<T, E, F> {

        protected final String        name;
        protected final Disruptor<T>  disruptor;
        protected final RingBuffer<T> ringBuffer;

        @SuppressWarnings("unchecked")
        public Batching(EventFactory<T> factory, int bufSize, String name, EventHandler<T> handler) {
            this.name = name;
            this.disruptor = new Disruptor<>(factory, bufSize, new NamedThreadFactory(name, true));
            this.disruptor.handleEventsWith(handler);
            this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(name));
            this.ringBuffer = this.disruptor.start();
        }

        public abstract boolean apply(final long regionId, final E message, final CompletableFuture<F> future);

        public void shutdown() {
            try {
                this.disruptor.shutdown(3L, TimeUnit.SECONDS);
            } catch (final Exception e) {
                LOG.error("Fail to shutdown {}, {}.", toString(), StackTraceUtil.stackTrace(e));
            }
        }

        @Override
        public String toString() {
            return "Batching{" + "name='" + name + '\'' + ", disruptor=" + disruptor + '}';
        }
    }

    private KVEntry getEntry(final long regionId, final byte op, final byte[] key) {
        Requires.requireTrue(regionId != ANY_REGION_ID);
        Requires.requireNonNull(key, "key");
        checkState();
        checkRegionRoute(regionId != ANY_REGION_ID);

        final CompletableFuture<KVEntry> future = new CompletableFuture<>();
        internalGetEntry(future, regionId, op, key, this.failoverRetries);
        return FutureHelper.get(future, this.futureTimeoutMillis);
    }

    private void internalGetEntry(final CompletableFuture<KVEntry> future,
                                    final long regionId, final byte op,
                                    final byte[] key, final int retriesLeft) {
        final Region region = (regionId == ANY_REGION_ID) ? this.pdClient.findRegionByKey(key, true) :
                              this.pdClient.getRegionById(regionId);
        final RegionEngine regionEngine = getRegionEngine(region.getId(), true);
        Requires.requireNonNull(regionEngine, "Only leader can call floorEntry");
        final RetryRunner retryRunner = retryCause -> internalGetEntry(future, regionId, op, key, retriesLeft - 1);
        final FailoverClosure<KVEntry> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
	if (ensureOnValidEpoch(region, regionEngine, closure)) {
            switch (op) {
                case KVOperation.FLOOR_ENTRY:
                    getRawKVStore(regionEngine).floorEntry(key, closure);
                    break;
                case KVOperation.LOWER_ENTRY:
                    getRawKVStore(regionEngine).lowerEntry(key, closure);
                    break;
                case KVOperation.CEILING_ENTRY:
                    getRawKVStore(regionEngine).ceilingEntry(key, closure);
                    break;
                case KVOperation.HIGHER_ENTRY:
                    getRawKVStore(regionEngine).higherEntry(key, closure);
                    break;
                default:
                    Requires.requireTrue(false, "Unsupported operator type " + KVOperation.opName(op));
            }
	}
    }

    @Override
    public KVEntry floorEntry(final long regionId, final byte[] key) {
        return getEntry(regionId, KVOperation.FLOOR_ENTRY, key);
    }

    @Override
    public KVEntry lowerEntry(final long regionId, final byte[] key) {
        return getEntry(regionId, KVOperation.LOWER_ENTRY, key);
    }

    @Override
    public KVEntry ceilingEntry(final long regionId, final byte[] key) {
        return getEntry(regionId, KVOperation.CEILING_ENTRY, key);
    }

    @Override
    public KVEntry higherEntry(final long regionId, final byte[] key) {
        return getEntry(regionId, KVOperation.HIGHER_ENTRY, key);
    }
}
