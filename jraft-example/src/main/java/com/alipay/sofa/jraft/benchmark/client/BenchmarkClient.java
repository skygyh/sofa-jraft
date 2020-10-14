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

import com.alipay.sofa.jraft.benchmark.BenchmarkHelper;
import com.alipay.sofa.jraft.benchmark.Yaml;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.codahale.metrics.ConsoleReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author jiachun.fjc
 */
public class BenchmarkClient {

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkClient.class);

    public static void main(final String[] args) {
        if (args.length < 7) {
            LOG.error("Args: [initialServerList], [configPath], [threads], [writeRatio], [readRatio], [valueSize] are needed.");
            System.exit(-1);
        }
        final String initialServerList = args[1];
        final String configPath = args[2];
        final boolean isClient = args.length >= 4 && Boolean.parseBoolean(args[3]);
        final int threads = args.length > 4 ? Integer.parseInt(args[4]) : 1;
        final int writeRatio = args.length > 5 ? Integer.parseInt(args[5]) : 5;
        final int readRatio = args.length > 6 ? Integer.parseInt(args[6]) : 5;
        final int keyCount = args.length > 7 ? Integer.parseInt(args[7]) : 10000000;
        final int keySize = args.length > 8 ? Integer.parseInt(args[8]) : 64;
        final int valueSize = args.length > 9 ? Integer.parseInt(args[9]) : 1024;

        final RheaKVStoreOptions opts = Yaml.readConfig(configPath);
        opts.setInitialServerList(initialServerList);
        final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
        if (!rheaKVStore.init(opts)) {
            LOG.error("Fail to init [RheaKVStore]");
            System.exit(-1);
        }

        final PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
        final List<RegionEngineOptions> regionEngineOptionsList = opts.getStoreEngineOptions()
            .getRegionEngineOptionsList();
        for (RegionEngineOptions regionEngineOptions : regionEngineOptionsList) {
            final long regionId = regionEngineOptions.getRegionId();
            LOG.info("Leader in region {} is {}", regionId, pdClient.getLeader(regionId, true, 30000));
        }
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
        }
        ;
        BenchmarkHelper.startBenchmark2(rheaKVStore, threads, writeRatio, readRatio, keyCount, keySize, valueSize,
            regionEngineOptionsList);

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

    private static void rebalance2(final RheaKVStore rheaKVStore, final String initialServerList,
                                   final List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        final Configuration configuration = new Configuration();
        configuration.parse(initialServerList);
        List<PeerId> peers = configuration.getPeers();

        try {
            Thread.sleep(3 * 1000);
            final PlacementDriverClient pdClient = rheaKVStore.getPlacementDriverClient();
            for (RegionRouteTableOptions regionRouteTableOpt : regionRouteTableOptionsList) {
                final long regionId = regionRouteTableOpt.getRegionId();
                pdClient.transferLeader(regionId, JRaftHelper.toPeer(peers.get((int) (regionId % peers.size()))), true);
                Endpoint leader = pdClient.getLeader(regionId, true, 20000);
                System.out.println(String.format("The region %d leader is: %s", regionId, leader));
            }
        } catch (Exception e) {
            LOG.error("Hit exception on rebalance ", e);
        }

    }
}
