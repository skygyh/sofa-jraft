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
package com.alipay.sofa.jraft.benchmark.server;

import com.alipay.sofa.jraft.benchmark.BenchmarkHelper;
import com.alipay.sofa.jraft.benchmark.Yaml;
import com.alipay.sofa.jraft.example.rheakv.Node;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.codahale.metrics.ConsoleReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jiachun.fjc
 */
public class BenchmarkServer {

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkServer.class);

    // Example command:
    // mvn -pl jraft-example exec:java -Dexec.mainClass=com.alipay.sofa.jraft.benchmark.server.BenchmarkServer -Dexec.classpathScope=test  -Dexec.args="server 10.247.97.240:18091,10.247.97.241:18091,10.247.97.242:18091 jraft-example/config/benchmark_server_pmem.yaml"
    public static void main(final String[] args) {
        if (args.length < 3) {
            LOG.error("[initialServerList] [configPath] <isClient> <threads> <writeRatio> <readRatio> <valueSize>\n" +
            "\t[initialServerList], [configPath] are mandatory.\n" +
            "\t<isClient> <threads> <writeRatio> <readRatio> <keyCount> <keySize> <valueSize> are optional when isClient is true.");
        }
        final String initialServerList = args[1];
        final String configPath = args[2];
        final boolean isClient = args.length >= 4 && Boolean.parseBoolean(args[3]);

        final RheaKVStoreOptions opts = Yaml.readConfig(configPath);
        opts.setInitialServerList(initialServerList);

        final Node node = new Node(opts);
        node.start();

        ConsoleReporter.forRegistry(KVMetrics.metricRegistry()) //
                .build() //
                .start(30, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        LOG.info("BenchmarkServer start OK, options: {}", opts);
        if (!isClient) {
            try {
                synchronized (node) {
                    node.wait();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (isClient) {
            LOG.info("BenchmarkServer act as a Client as well ...");
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {};
            final int threads = args.length > 4 ? Integer.parseInt(args[4]) : 1;
            final int writeRatio = args.length > 5 ? Integer.parseInt(args[5]) : 5;
            final int readRatio = args.length > 6 ? Integer.parseInt(args[6]) : 5;
            final int keyCount = args.length > 7 ? Integer.parseInt(args[7]) : 10000000;
            final int keySize = args.length > 8 ? Integer.parseInt(args[8]) : 64;
            final int valueSize = args.length > 9 ? Integer.parseInt(args[9]) : 1024;
            final PlacementDriverClient pdClient = node.getRheaKVStore().getPlacementDriverClient();
            final List<RegionEngineOptions> regionEngineOptionsList = opts.getStoreEngineOptions().getRegionEngineOptionsList();
            for (RegionEngineOptions regionEngineOptions : regionEngineOptionsList) {
                final long regionId = regionEngineOptions.getRegionId();
                LOG.info("Leader in region {} is {}", regionId, pdClient.getLeader(regionId, true, 30000));
            }
                      try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {}; 
            BenchmarkHelper.startBenchmark2(node.getRheaKVStore(),
                    threads,
                    writeRatio,
                    readRatio,
                    keyCount,
                    keySize,
                    valueSize,
                    regionEngineOptionsList);
        }
    }
}
