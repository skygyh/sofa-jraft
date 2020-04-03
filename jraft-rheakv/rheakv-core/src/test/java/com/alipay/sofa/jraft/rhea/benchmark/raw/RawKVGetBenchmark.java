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
package com.alipay.sofa.jraft.rhea.benchmark.raw;

import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.*;

/**
 * @author jiachun.fjc
 */
@State(Scope.Benchmark)
public abstract class RawKVGetBenchmark extends BaseRawStoreBenchmark {
    /**
     //
     // 100w keys, each value is 100 bytes.
     //
     // Memory get tps = 1166.701 * 1000 = 1166701 ops/second
     // PMem   get tps =  211.683 * 1000 =  211683 ops/second
     //
     MemoryRawKVGetBenchmark.get               thrpt       3  1166.701 ± 1086.047  ops/ms
     PMemRawKVGetBenchmark.get                 thrpt       3   211.683 ±  105.283  ops/ms
     MemoryRawKVGetBenchmark.get                avgt       3     0.001 ±    0.001   ms/op
     PMemRawKVGetBenchmark.get                  avgt       3     0.005 ±    0.003   ms/op
     MemoryRawKVGetBenchmark.get              sample  916115     0.001 ±    0.001   ms/op
     MemoryRawKVGetBenchmark.get:get·p0.00    sample            ≈ 10⁻³              ms/op
     MemoryRawKVGetBenchmark.get:get·p0.50    sample             0.001              ms/op
     MemoryRawKVGetBenchmark.get:get·p0.90    sample             0.001              ms/op
     MemoryRawKVGetBenchmark.get:get·p0.95    sample             0.001              ms/op
     MemoryRawKVGetBenchmark.get:get·p0.99    sample             0.001              ms/op
     MemoryRawKVGetBenchmark.get:get·p0.999   sample             0.013              ms/op
     MemoryRawKVGetBenchmark.get:get·p0.9999  sample             0.020              ms/op
     MemoryRawKVGetBenchmark.get:get·p1.00    sample             7.823              ms/op
     PMemRawKVGetBenchmark.get                sample  816941     0.005 ±    0.001   ms/op
     PMemRawKVGetBenchmark.get:get·p0.00      sample             0.003              ms/op
     PMemRawKVGetBenchmark.get:get·p0.50      sample             0.004              ms/op
     PMemRawKVGetBenchmark.get:get·p0.90      sample             0.006              ms/op
     PMemRawKVGetBenchmark.get:get·p0.95      sample             0.006              ms/op
     PMemRawKVGetBenchmark.get:get·p0.99      sample             0.008              ms/op
     PMemRawKVGetBenchmark.get:get·p0.999     sample             0.022              ms/op
     PMemRawKVGetBenchmark.get:get·p0.9999    sample             0.032              ms/op
     PMemRawKVGetBenchmark.get:get·p1.00      sample             6.775              ms/op
     MemoryRawKVGetBenchmark.get                  ss       3     0.038 ±    0.225   ms/op
     PMemRawKVGetBenchmark.get                    ss       3     0.032 ±    0.202   ms/op
     */
    protected abstract RawKVStore rawKVStore();

    @Setup
    public void setup() {
        try {
            super.setup();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // insert data first
        put();
    }

    @TearDown
    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void get() {
        byte[] key = BytesUtil.writeUtf8("benchmark_" + getRandomInt());
        rawKVStore().get(key, false, null);
    }

    public void put() {
        final int batchSize = 100;
        final List<KVEntry> batch = Lists.newArrayListWithCapacity(batchSize);
        for (int i = 0; i < KEY_COUNT; i += batchSize) {
            byte[] key = BytesUtil.writeUtf8("benchmark_" + i);
            batch.add(new KVEntry(key, VALUE_BYTES));
            if (batch.size() >= batchSize) {
                rawKVStore().put(batch, null);
                batch.clear();
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(RawKVGetBenchmark.class.getSimpleName()) //
            .warmupIterations(1) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .threads(CONCURRENCY) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }
}
