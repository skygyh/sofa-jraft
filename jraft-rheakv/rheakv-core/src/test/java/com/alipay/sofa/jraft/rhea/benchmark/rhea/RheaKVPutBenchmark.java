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

import com.alipay.sofa.jraft.util.BytesUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.CONCURRENCY;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.VALUE_BYTES;

/**
 * @author jiachun.fjc
 */
@State(Scope.Benchmark)
public class RheaKVPutBenchmark extends RheaBenchmarkCluster {

    /**
     //
     // 100w keys, each value is 512 bytes.
     //
     // PMem put tps = 4.807 * 1000 = 4807 ops/second
     // Single thread
     Benchmark                             Mode    Cnt    Score   Error   Units
     RheaKVPutBenchmark.put               thrpt           4.807          ops/ms
     RheaKVPutBenchmark.put                avgt           0.204           ms/op
     RheaKVPutBenchmark.put              sample  47407    0.211 ± 0.011   ms/op
     RheaKVPutBenchmark.put:put·p0.00    sample           0.132           ms/op
     RheaKVPutBenchmark.put:put·p0.50    sample           0.195           ms/op
     RheaKVPutBenchmark.put:put·p0.90    sample           0.238           ms/op
     RheaKVPutBenchmark.put:put·p0.95    sample           0.261           ms/op
     RheaKVPutBenchmark.put:put·p0.99    sample           0.372           ms/op
     RheaKVPutBenchmark.put:put·p0.999   sample           1.952           ms/op
     RheaKVPutBenchmark.put:put·p0.9999  sample           5.730           ms/op
     RheaKVPutBenchmark.put:put·p1.00    sample         112.198           ms/op
     RheaKVPutBenchmark.put                  ss           3.272           ms/op

     */

    @Setup
    public void setup() {
        try {
            super.start();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @TearDown
    public void tearDown() {
        try {
            super.shutdown();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void put() {
        int k = getRandomInt();
        byte[] key = BytesUtil.writeUtf8("benchmark_" + k);
        final long regionId = regionIds.get(k % regionIds.size());
        regionId2Store.get(regionId).bPut(regionId, key, VALUE_BYTES);
    }

    public static void main(String[] args) throws RunnerException {
        int concurrency = CONCURRENCY;
        if (args != null && args.length > 0) {
            concurrency = Integer.parseInt(args[0]);
        }
        Options opt = new OptionsBuilder() //
            .include(RheaKVPutBenchmark.class.getSimpleName()) //
            .warmupIterations(1) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(1) //
            .measurementTime(TimeValue.seconds(10)) //
            .threads(concurrency) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }
}
