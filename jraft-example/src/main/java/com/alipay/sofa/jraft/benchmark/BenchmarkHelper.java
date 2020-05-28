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
package com.alipay.sofa.jraft.benchmark;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.Bits;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class BenchmarkHelper {
    private static final byte[] BYTES    = new byte[] { 0, 1 };

    private static final Meter  putMeter = KVMetrics.meter("put_benchmark_tps");
    private static final Meter  getMeter = KVMetrics.meter("get_benchmark_tps");
    private static final Timer  putTimer = KVMetrics.timer("put_benchmark_timer");
    private static final Timer  getTimer = KVMetrics.timer("get_benchmark_timer");
    private static final Timer  timer    = KVMetrics.timer("benchmark_timer");
    private static final Logger LOG      = LoggerFactory.getLogger(BenchmarkHelper.class);
    private static final  AtomicInteger  submittedKey = new AtomicInteger(0);

    public static void startBenchmark(final RheaKVStore rheaKVStore, final int threads, final int writeRatio, final int readRatio,
                                      final int valueSize, final List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        for (int i = 0; i < threads; i++) {
            final Thread t = new Thread(() -> doRequest(rheaKVStore, writeRatio, readRatio, valueSize, regionRouteTableOptionsList));
            t.setDaemon(false);
            t.start();
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void doRequest(final RheaKVStore rheaKVStore,
                                 final int writeRatio,
                                 final int readRatio,
                                 final int valueSize,
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

    public static CompletableFuture<Boolean> put(final RheaKVStore rheaKVStore, final long regionId, final byte[] key,
                                                 final byte[] value) {
        return rheaKVStore.put(regionId, key, value);
    }

    public static CompletableFuture<byte[]> get(final RheaKVStore rheaKVStore, final long regionId, final byte[] key) {
        return rheaKVStore.get(regionId, key);
    }

    public static void startBenchmark2(final RheaKVStore rheaKVStore,
                                       final int threads,
                                       final int writeRatio,
                                       final int readRatio,
                                       final int keyCount,
                                       final int keySize,
                                       final int valueSize,
                                       final List<RegionEngineOptions> regionEngineOptionsList) {

        for (int i = 0; i < threads; i++) {
            final Thread t = new Thread(() -> doRequest2(rheaKVStore, writeRatio, readRatio, keyCount, keySize, valueSize, regionEngineOptionsList));
            t.setDaemon(false);
            t.start();
        }

    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void doRequest2(final RheaKVStore rheaKVStore,
                                  final int writeRatio,
                                  final int readRatio,
                                  final int keyCount,
                                  final int keySize,
                                  final int valueSize,
                                  final List<RegionEngineOptions> regionEngineOptionsList) {
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
            Bits.putInt(keyBytes, 0, k);
            final Timer.Context ctx = timer.time();
            if (Math.abs(index++ % sum) < writeRatio) {
                // put
                final Timer.Context putCtx = putTimer.time();
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

                if (submittedKey.get() >= keyCount) {
                    LOG.error("submitted key: {}", submittedKey.get());
                    return;
                }

            } else {
                // get
                final Timer.Context getCtx = getTimer.time();
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
            }
        }
    }
}
