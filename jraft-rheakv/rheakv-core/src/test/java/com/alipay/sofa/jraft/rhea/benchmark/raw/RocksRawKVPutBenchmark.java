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

import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;

public class RocksRawKVPutBenchmark extends RawKVPutBenchmark {
    protected String          tempPath;
    protected RocksRawKVStore rocksRawKVStore;
    protected RocksDBOptions  dbOptions;

    @Override
    protected RawKVStore initRawKVStore() throws IOException {
        File file = getTempDir();
        this.tempPath = file.getAbsolutePath();
        System.out.println(this.tempPath);
        this.rocksRawKVStore = new RocksRawKVStore();
        this.dbOptions = new RocksDBOptions();
        this.dbOptions.setDbPath(this.tempPath);
        this.dbOptions.setSync(true);
        this.rocksRawKVStore.init(this.dbOptions);
        return this.rocksRawKVStore;
    }

    @Override
    protected RawKVStore rawKVStore() {
        return this.rocksRawKVStore;
    }

    @Override
    protected void shutdown() throws IOException {
        this.rocksRawKVStore.shutdown();
        FileUtils.deleteQuietly(new File((this.tempPath)));
    }

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
}
