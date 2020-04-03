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
package com.alipay.sofa.jraft.rhea.benchmark.rhea;

import com.alipay.remoting.config.Configs;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.errors.NotLeaderException;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 * @author jiachun.fjc
 */
public class RheaBenchmarkCluster {
    // Default path if system environment variables (DB_PATH and RAFT_PATH) are not set.
    private static final String                        DefaultBenchMarkDbPath   = "/mnt/mem/benchmark_rhea_db";
    private static final String                        DefaultBenchMarkRaftPath = "/mnt/mem/benchmark_rhea_raft";

    private static final String[]                      CONF                     = {
            "benchmark/conf/rhea_cluster_1.yaml", "benchmark/conf/rhea_cluster_2.yaml",
            "benchmark/conf/rhea_cluster_3.yaml"                               };

    private volatile String                            tempDbPath;
    private volatile String                            tempRaftPath;
    protected CopyOnWriteArrayList<RheaKVStore>        stores                   = new CopyOnWriteArrayList<>();
    protected CopyOnWriteArrayList<RheaKVStoreOptions> opts                     = new CopyOnWriteArrayList<>();
    protected List<Long>                               regionIds                = new ArrayList<>();
    protected Map<Long, RheaKVStore>                   regionId2Store           = new HashMap<>();
    protected int[]                                    numbers;
    protected int                                      index;

    protected int getRandomInt() {
        return this.numbers[this.index++ % this.numbers.length];
    }

    protected void start() throws IOException, InterruptedException {
        SystemPropertyUtil.setProperty(Configs.NETTY_BUFFER_LOW_WATERMARK, Integer.toString(256 * 1024));
        SystemPropertyUtil.setProperty(Configs.NETTY_BUFFER_HIGH_WATERMARK, Integer.toString(512 * 1024));
        File file = new File(DefaultBenchMarkDbPath);
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        String dbPath = System.getenv("DB_PATH");
        if (dbPath == null || dbPath.isEmpty()) {
            dbPath = DefaultBenchMarkDbPath;
        }
        file = new File(dbPath);
        if (file.mkdir()) {
            this.tempDbPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempDbPath);
        }

        String raftPath = System.getenv("RAFT_PATH");
        if (raftPath == null || raftPath.isEmpty()) {
            raftPath = DefaultBenchMarkRaftPath;
            System.out.println("make dir: " + this.tempRaftPath);
        }
        file = new File(raftPath);
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File(raftPath);
        if (file.mkdir()) {
            this.tempRaftPath = file.getAbsolutePath();
        }
        for (String c : CONF) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            final RheaKVStoreOptions opt = mapper.readValue(new File(c), RheaKVStoreOptions.class);
            RheaKVStore rheaKVStore = new DefaultRheaKVStore();
            if (rheaKVStore.init(opt)) {
                stores.add(rheaKVStore);
                System.out.println("RheaKVStoreOptions : " + opt);
            } else {
                throw new RuntimeException("Fail to init rhea kv store witch conf: " + c);
            }
            opts.add(opt);
        }

        this.numbers = BenchmarkUtil.buildRandomNumbers(BenchmarkUtil.KEY_COUNT);

        final Configuration configuration = new Configuration();
        configuration.parse(opts.get(0).getInitialServerList());
        List<PeerId> peers = configuration.getPeers();

        RheaKVStoreOptions opt = opts.get(0);
        Thread.sleep(opt.getStoreEngineOptions().getCommonNodeOptions().getElectionTimeoutMs() * 2);
        for (RegionEngineOptions regionEngineOpt : opt.getStoreEngineOptions().getRegionEngineOptionsList()) {
            final long regionId = regionEngineOpt.getRegionId();
            regionIds.add(regionId);
            final PlacementDriverClient pdClient = getLeaderStore(regionId).getPlacementDriverClient();
            pdClient.transferLeader(regionId, JRaftHelper.toPeer(peers.get((int) (regionId % peers.size()))), true);
            Endpoint leader = pdClient.getLeader(regionId, true, 10000);
            System.out.println(String.format("The region %d leader is: %s", regionId, leader));

        }

        for (Long regionId : regionIds) {
            this.regionId2Store.put(regionId, getLeaderStore(regionId));
        }
    }

    protected void shutdown() throws IOException {
        for (RheaKVStore store : stores) {
            store.shutdown();
        }
        if (this.tempDbPath != null) {
            System.out.println("removing dir: " + this.tempDbPath);
            FileUtils.forceDelete(new File(this.tempDbPath));
        }
        if (this.tempRaftPath != null) {
            System.out.println("removing dir: " + this.tempRaftPath);
            FileUtils.forceDelete(new File(this.tempRaftPath));
        }
    }

    public RheaKVStore getLeaderStore(long regionId) {
        for (int i = 0; i < 20; i++) {
            for (RheaKVStore store : stores) {
                if (((DefaultRheaKVStore) store).isLeader(regionId)) {
                    return store;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        System.out.println("fail to find leader for region " + regionId);
        throw new NotLeaderException("no leader");
    }

    public RheaKVStore getFollowerStore(long regionId) {
        for (int i = 0; i < 20; i++) {
            for (RheaKVStore store : stores) {
                if (!((DefaultRheaKVStore) store).isLeader(regionId)) {
                    return store;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        System.out.println("fail to find follower for region " + regionId);
        throw new NotLeaderException("no follower");
    }
}
