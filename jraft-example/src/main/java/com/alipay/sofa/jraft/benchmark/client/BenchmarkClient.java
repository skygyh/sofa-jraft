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
package com.alipay.sofa.jraft.benchmark.client;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alipay.sofa.jraft.benchmark.BenchmarkHelper;
import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.util.Bits;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.benchmark.Yaml;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Timer;

/**
 * @author jiachun.fjc
 */
public class BenchmarkClient {

    private static final Logger        LOG          = LoggerFactory.getLogger(BenchmarkClient.class);

    private static final byte[]        BYTES        = new byte[] { 0, 1 };

    private static final Meter         putMeter     = KVMetrics.meter("put_benchmark_tps");
    private static final Meter         getMeter     = KVMetrics.meter("get_benchmark_tps");
    private static final Timer         putTimer     = KVMetrics.timer("put_benchmark_timer");
    private static final Timer         getTimer     = KVMetrics.timer("get_benchmark_timer");
    private static final Timer         timer        = KVMetrics.timer("benchmark_timer");

    private static final AtomicInteger submittedKey = new AtomicInteger(0);
    private static String              DEFAULT_MODE = "async";

    public static void main(final String[] args) {
        if (args.length < 8) {
            LOG.error("Args: [initialServerList], [configPath], [threads], [writeRatio], [readRatio],[keycount], [keysize], [valueSize] are needed.");
            System.exit(-1);
        }
        final String initialServerList = args[1];
        final String configPath = args[2];

        final int threads = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        final int writeRatio = args.length > 4 ? Integer.parseInt(args[4]) : 5;
        final int readRatio = args.length > 5 ? Integer.parseInt(args[5]) : 5;
        final int keyCount = args.length > 6 ? Integer.parseInt(args[6]) : 10000000;
        final int keySize = args.length > 7 ? Integer.parseInt(args[7]) : 64;
        final int valueSize = args.length > 8 ? Integer.parseInt(args[8]) : 1024;
        final String opMode = args.length > 9 ? args[9] : DEFAULT_MODE;

        final RheaKVStoreOptions opts = Yaml.readConfig(configPath);
        opts.setInitialServerList(initialServerList);
        final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
        if (!rheaKVStore.init(opts)) {
            LOG.error("Fail to init [RheaKVStore]");
            System.exit(-1);
        }

        final List<RegionRouteTableOptions> regionRouteTableOptionsList = opts.getPlacementDriverOptions()
            .getRegionRouteTableOptionsList();
        String localIP = null;
        try {
            localIP = Inet4Address.getLocalHost().getHostAddress();
            LOG.info("local IP {}", localIP);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        final List<RegionRouteTableOptions> localRegionRouteTableOptionsList = new ArrayList<>();
        final PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        for (RegionRouteTableOptions regionRouteTableOptions : regionRouteTableOptionsList) {
            final long regionId = regionRouteTableOptions.getRegionId();
            Endpoint ep = null;
            try {
                ep = pdClient.getLeader(regionId, true, 30000);
                LOG.info("regionId: {},ep: {}", regionId, ep);
            } catch (RouteTableException e) {
                LOG.info("no leader in regionId {}", regionId);
                //e.printStackTrace();
            }
            if (localIP != null && ep != null) {
                if (localIP.equals(ep.getIp())) {
                    localRegionRouteTableOptionsList.add(regionRouteTableOptions);
                    LOG.info("local Leader in region {} is {}", regionId, ep);
                }
            }
        }

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
        }
        ;

        // rebalance(rheaKVStore, initialServerList, regionRouteTableOptionsList);

        //rheaKVStore.bPut("benchmark", BytesUtil.writeUtf8("benchmark start at: " + new Date()));
        //LOG.info(BytesUtil.readUtf8(rheaKVStore.bGet("benchmark")));
        //        try {
        //            System.out.println(Inet4Address.getLocalHost().getHostAddress());
        //        } catch (UnknownHostException e) {
        //            e.printStackTrace();
        //        }
        ConsoleReporter.forRegistry(KVMetrics.metricRegistry()) //
            .build() //
            .start(30, TimeUnit.SECONDS);

        LOG.info("Start benchmark...");
        startBenchmark_hash(rheaKVStore, threads, writeRatio, readRatio, keyCount, keySize, valueSize,
            localRegionRouteTableOptionsList, opMode);
    }

    public static void startBenchmark_hash(final RheaKVStore rheaKVStore,
                                       final int threads,
                                       final int writeRatio,
                                       final int readRatio,
                                       final int keyCount,
                                       final int keySize,
                                       final int valueSize,
                                       final List<RegionRouteTableOptions>  regionEngineOptionsList,
                                       final String opMode) {

        for (int i = 0; i < threads; i++) {
            final Thread t = new Thread(() -> doRequest_hash(rheaKVStore, writeRatio, readRatio, keyCount, keySize, valueSize, regionEngineOptionsList, opMode));
            //t.setDaemon(false);
            t.start();
        }

    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void doRequest_hash(final RheaKVStore rheaKVStore,
                                  final int writeRatio,
                                  final int readRatio,
                                  final int keyCount,
                                  final int keySize,
                                  final int valueSize,
                                  final List<RegionRouteTableOptions>  regionEngineOptionsList, final String opMode) {
        final int regionSize = regionEngineOptionsList.size();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int sum = writeRatio + readRatio;
        final Semaphore slidingWindow = new Semaphore(sum);
        int index = 0;
        final byte[] keyBytes = new byte[keySize];
        final byte[] valeBytes = new byte[valueSize];
        random.nextBytes(valeBytes);
        final AtomicInteger failure = new AtomicInteger();
        for (;;) {
            if (failure.get() > sum) {
                try {
                    // throttle a while
                    LOG.error("Throttled, give it a break");
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {}
                failure.set(0);
            }
            try {
                slidingWindow.acquire();
            } catch (final Exception e) {
                LOG.error("Wrong slidingWindow: {}, {}", slidingWindow.toString(), StackTraceUtil.stackTrace(e));
            }
            int k = random.nextInt(keyCount);
            int regionIndex = k % regionSize;
            final long regionId = regionEngineOptionsList.get(regionIndex).getRegionId();
           // LOG.info("regiondId {}", regionId);
            Bits.putInt(keyBytes, 0, k);
            final Timer.Context ctx = timer.time();
            if (Math.abs(index++ % sum) < writeRatio) {
                // put
                final Timer.Context putCtx = putTimer.time();

                if (opMode.equals(DEFAULT_MODE)) {
                    final CompletableFuture<Boolean> f = put(rheaKVStore, regionId, keyBytes, valeBytes);
                    f.whenComplete((result, throwable) -> {

                        if (!result || throwable != null) {
                            failure.incrementAndGet();
                        } else {
                            putCtx.stop();
                            ctx.stop();
                            putMeter.mark();
                            submittedKey.incrementAndGet();
                        }
                        slidingWindow.release();
                    });
                } else  {
                    final Boolean rc = bput(rheaKVStore, regionId, keyBytes, valeBytes);
                    if (rc != true) {
                        failure.incrementAndGet();
                    } else {
                        putCtx.stop();
                        ctx.stop();
                        putMeter.mark();
                        submittedKey.incrementAndGet();
                    }
                    slidingWindow.release();
                }

                if (submittedKey.get() >= keyCount) {
                    LOG.info("submitted key: {}", submittedKey.get());
                    System.exit(0);
                }

            } else {
                // get
                final Timer.Context getCtx = getTimer.time();
                if (opMode.equals(DEFAULT_MODE)) {
                    final CompletableFuture<byte[]> f = get(rheaKVStore, regionId, keyBytes);
                    f.whenComplete((ignored, throwable) -> {
                        if (throwable != null) {
                            failure.incrementAndGet();
                        } else {
                            getCtx.stop();
                            ctx.stop();
                            getMeter.mark();
                        }
                        slidingWindow.release();
                    });
                } else {
                    try {
                        final byte[] valueBytes = bget(rheaKVStore, regionId, keyBytes);
                        getCtx.stop();
                        ctx.stop();
                        getMeter.mark();
                    } catch (Exception e) {
                        failure.incrementAndGet();
                    }

                    slidingWindow.release();
                }
            }
        }
    }

    public static CompletableFuture<Boolean> put(final RheaKVStore rheaKVStore, final long regionId, final byte[] key,
                                                 final byte[] value) {
        return rheaKVStore.put(regionId, key, value);
    }

    public static Boolean bput(final RheaKVStore rheaKVStore, final long regionId, final byte[] key, final byte[] value) {
        List<KVEntry> kvEntries = new ArrayList<>();
        kvEntries.add(new KVEntry(regionId, key, value));
        return rheaKVStore.bPut(kvEntries);
    }

    public static byte[] bget(final RheaKVStore rheaKVStore, final long regionId, final byte[] key) {
        return rheaKVStore.bGet(regionId, key);
    }

    public static CompletableFuture<byte[]> get(final RheaKVStore rheaKVStore, final long regionId, final byte[] key) {
        return rheaKVStore.get(regionId, key);
    }

    public static void startBenchmark(final RheaKVStore rheaKVStore, final int threads, final int writeRatio, final int readRatio,
                                      final int valueSize, final List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        for (int i = 0; i < threads; i++) {
            final Thread t = new Thread(() -> doRequest(rheaKVStore, writeRatio, readRatio, valueSize, regionRouteTableOptionsList));
            t.setDaemon(true);
            t.start();
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void doRequest(final RheaKVStore rheaKVStore, final int writeRatio, final int readRatio, final int valueSize,
                                 final List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        final int regionSize = regionRouteTableOptionsList.size();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int sum = writeRatio + readRatio;
        final Semaphore slidingWindow = new Semaphore(sum);
        int index = 0;
        int randomRegionIndex = 0;
        final byte[] valeBytes = new byte[valueSize];
        random.nextBytes(valeBytes);
        for (;;) {
            try {
                slidingWindow.acquire();
            } catch (final Exception e) {
                LOG.error("Wrong slidingWindow: {}, {}", slidingWindow.toString(), StackTraceUtil.stackTrace(e));
            }
            int i = index++;
            if (i % sum == 0) {
                randomRegionIndex = random.nextInt(regionSize);
            }
            byte[] keyBytes = regionRouteTableOptionsList.get(randomRegionIndex).getStartKeyBytes();
            if (keyBytes == null) {
                keyBytes = BYTES;
            }
            final Timer.Context ctx = timer.time();
            if (Math.abs(i % sum) < writeRatio) {
                // put
                final Timer.Context putCtx = putTimer.time();
                final CompletableFuture<Boolean> f = put(rheaKVStore, keyBytes, valeBytes);
                f.whenComplete((ignored, throwable) -> {
                    slidingWindow.release();
                    ctx.stop();
                    putCtx.stop();
                });
            } else {
                // get
                final Timer.Context getCtx = getTimer.time();
                final CompletableFuture<byte[]> f = get(rheaKVStore, keyBytes);
                f.whenComplete((ignored, throwable) -> {
                    slidingWindow.release();
                    ctx.stop();
                    getCtx.stop();
                });
            }
        }
    }

    public static CompletableFuture<Boolean> put(final RheaKVStore rheaKVStore, final byte[] key, final byte[] value) {
        return rheaKVStore.put(key, value);
    }

    public static CompletableFuture<byte[]> get(final RheaKVStore rheaKVStore, final byte[] key) {
        return rheaKVStore.get(key);
    }

    // Because we use fake PD, so we need manual rebalance
    public static void rebalance(final RheaKVStore rheaKVStore, final String initialServerList,
                                 final List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        final PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        final Configuration configuration = new Configuration();
        configuration.parse(initialServerList);
        final int serverSize = configuration.size();
        final int regionSize = regionRouteTableOptionsList.size();
        final int regionSizePerServer = regionSize / serverSize;
        final Queue<Long> regions = new ArrayDeque<>();
        for (final RegionRouteTableOptions r : regionRouteTableOptionsList) {
            regions.add(r.getRegionId());
        }
        final Map<PeerId, Integer> peerMap = Maps.newHashMap();
        for (;;) {
            final Long regionId = regions.poll();
            if (regionId == null) {
                break;
            }
            PeerId peerId;
            try {
                final Endpoint endpoint = pdClient.getLeader(regionId, true, 10000);
                if (endpoint == null) {
                    continue;
                }
                peerId = new PeerId(endpoint, 0);
                LOG.info("Region {} leader is {}", regionId, peerId);
            } catch (final Exception e) {
                regions.add(regionId);
                continue;
            }
            final Integer size = peerMap.get(peerId);
            if (size == null) {
                peerMap.put(peerId, 1);
                continue;
            }
            if (size < regionSizePerServer) {
                peerMap.put(peerId, size + 1);
                continue;
            }
            for (final PeerId p : configuration.listPeers()) {
                final Integer pSize = peerMap.get(p);
                if (pSize != null && pSize >= regionSizePerServer) {
                    continue;
                }
                try {
                    pdClient.transferLeader(regionId, JRaftHelper.toPeer(p), true);
                    LOG.info("Region {} transfer leader to {}", regionId, p);
                    regions.add(regionId);
                    break;
                } catch (final Exception e) {
                    LOG.error("Fail to transfer leader to {}", p);
                }
            }
        }

        for (final RegionRouteTableOptions r : regionRouteTableOptionsList) {
            final Long regionId = r.getRegionId();
            try {
                final Endpoint endpoint = pdClient.getLeader(regionId, true, 10000);
                LOG.info("Finally, the region: {} leader is: {}", regionId, endpoint);
            } catch (final Exception e) {
                LOG.error("Fail to get leader: {}", StackTraceUtil.stackTrace(e));
            }
        }
    }
}
