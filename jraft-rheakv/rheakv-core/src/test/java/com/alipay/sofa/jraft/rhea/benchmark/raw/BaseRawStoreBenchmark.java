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
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static com.alipay.sofa.jraft.rhea.benchmark.BenchmarkUtil.KEY_COUNT;

public abstract class BaseRawStoreBenchmark {

    protected abstract RawKVStore initRawKVStore() throws IOException;

    protected abstract void shutdown() throws IOException;

    private int[] numbers;
    private int   index;

    protected void setup() throws Exception {
        buildRandomNumbers(KEY_COUNT);
        initRawKVStore();
    }

    private void buildRandomNumbers(final int keyCount) {
        this.numbers = new int[keyCount];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < keyCount; i++) {
            this.numbers[i] = random.nextInt(keyCount);
        }
    }

    protected int getRandomInt() {
        return this.numbers[this.index++ % this.numbers.length];
    }

    protected File getTempDir() throws IOException {
        final File file = File.createTempFile("RawRocksDBTest", "test");
        FileUtils.forceDelete(file);
        if (file.mkdirs()) {
            return file;
        }
        throw new RuntimeException("fail to make dirs");
    }

    protected void tearDown() throws Exception {
        shutdown();
    }
}
