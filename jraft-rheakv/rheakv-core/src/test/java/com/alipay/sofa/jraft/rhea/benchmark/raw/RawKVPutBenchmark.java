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

import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.CONCURRENCY;
import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.VALUE_BYTES;

/**
 * @author jiachun.fjc
 */
@State(Scope.Benchmark)
public abstract class RawKVPutBenchmark extends BaseRawStoreBenchmark {

    /**
     //
     // 100w keys, each value is 512 bytes.
     //
     // Memory  put tps = 539.555 * 1000 = 539555 ops/second
     // PMem    put tps = 103.304 * 1000 = 103304 ops/second
     // RocksDB put tps =  1.727  * 1000 = 1727 ops/second
     //
     Benchmark                                  Mode     Cnt    Score     Error   Units
     MemoryRawKVPutBenchmark.put               thrpt       3  539.555 ± 100.526  ops/ms
     PMemRawKVPutBenchmark.put                 thrpt       3  103.304 ± 202.028  ops/ms
     RocksRawKVPutBenchmark.put                thrpt       3    1.727 ±   0.492  ops/ms
     MemoryRawKVPutBenchmark.put                avgt       3    0.002 ±   0.001   ms/op
     PMemRawKVPutBenchmark.put                  avgt       3    0.010 ±   0.016   ms/op
     RocksRawKVPutBenchmark.put                 avgt       3    0.580 ±   0.147   ms/op
     MemoryRawKVPutBenchmark.put              sample  900822    0.002 ±   0.001   ms/op
     MemoryRawKVPutBenchmark.put:put·p0.00    sample            0.001             ms/op
     MemoryRawKVPutBenchmark.put:put·p0.50    sample            0.002             ms/op
     MemoryRawKVPutBenchmark.put:put·p0.90    sample            0.003             ms/op
     MemoryRawKVPutBenchmark.put:put·p0.95    sample            0.003             ms/op
     MemoryRawKVPutBenchmark.put:put·p0.99    sample            0.004             ms/op
     MemoryRawKVPutBenchmark.put:put·p0.999   sample            0.018             ms/op
     MemoryRawKVPutBenchmark.put:put·p0.9999  sample            0.023             ms/op
     MemoryRawKVPutBenchmark.put:put·p1.00    sample           27.427             ms/op
     PMemRawKVPutBenchmark.put                sample  884142    0.008 ±   0.001   ms/op
     PMemRawKVPutBenchmark.put:put·p0.00      sample            0.005             ms/op
     PMemRawKVPutBenchmark.put:put·p0.50      sample            0.008             ms/op
     PMemRawKVPutBenchmark.put:put·p0.90      sample            0.009             ms/op
     PMemRawKVPutBenchmark.put:put·p0.95      sample            0.013             ms/op
     PMemRawKVPutBenchmark.put:put·p0.99      sample            0.019             ms/op
     PMemRawKVPutBenchmark.put:put·p0.999     sample            0.036             ms/op
     PMemRawKVPutBenchmark.put:put·p0.9999    sample            0.172             ms/op
     PMemRawKVPutBenchmark.put:put·p1.00      sample            2.474             ms/op
     RocksRawKVPutBenchmark.put               sample   50794    0.590 ±   0.031   ms/op
     RocksRawKVPutBenchmark.put:put·p0.00     sample            0.205             ms/op
     RocksRawKVPutBenchmark.put:put·p0.50     sample            0.264             ms/op
     RocksRawKVPutBenchmark.put:put·p0.90     sample            0.785             ms/op
     RocksRawKVPutBenchmark.put:put·p0.95     sample            3.461             ms/op
     RocksRawKVPutBenchmark.put:put·p0.99     sample            3.715             ms/op
     RocksRawKVPutBenchmark.put:put·p0.999    sample            4.071             ms/op
     RocksRawKVPutBenchmark.put:put·p0.9999   sample           83.045             ms/op
     RocksRawKVPutBenchmark.put:put·p1.00     sample          260.309             ms/op
     MemoryRawKVPutBenchmark.put                  ss       3    0.051 ±   0.423   ms/op
     PMemRawKVPutBenchmark.put                    ss       3    0.044 ±   0.346   ms/op
     RocksRawKVPutBenchmark.put                   ss       3    0.399 ±   1.174   ms/op

     * </dev/shm>
     */

    protected abstract RawKVStore rawKVStore();

    @Setup
    public void setup() {
        try {
            super.setup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @TearDown
    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder() //
            .include(RawKVPutBenchmark.class.getSimpleName()) //
            .warmupIterations(1) //
            .warmupTime(TimeValue.seconds(10)) //
            .measurementIterations(3) //
            .measurementTime(TimeValue.seconds(10)) //
            .threads(CONCURRENCY) //
            .forks(1) //
            .build();

        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void put() {
        byte[] key = BytesUtil.writeUtf8("benchmark_" + getRandomInt());
        rawKVStore().put(key, VALUE_BYTES, null);
    }
}
