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

import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.storage.PMemRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;

import java.nio.file.Paths;

public class PMemRawKVGetBenchmark extends RawKVGetBenchmark {
    private PMemRawKVStore pmemRawKVStore;

    @Override
    protected RawKVStore initRawKVStore() {
        PMemDBOptions pmemOpts = new PMemDBOptions();
        String dbPath = System.getenv("DB_PATH");
        if (dbPath == null || dbPath.isEmpty()) {
            dbPath = Paths.get(pmemOpts.getDbPath() == null ? PMemDBOptions.PMEM_ROOT_PATH : pmemOpts.getDbPath(),
                "PMemRawKVGetBenchmark_db").toString();
        }
        pmemOpts.setDbPath(dbPath);
        // data file size must be >= KEY_COUNT * KVSIZE + overhead
        pmemOpts.setPmemDataSize(1536 * 1024 * 1024);
        this.pmemRawKVStore = new PMemRawKVStore();
        this.pmemRawKVStore.init(pmemOpts);
        return this.pmemRawKVStore;
    }

    @Override
    protected RawKVStore rawKVStore() {
        return this.pmemRawKVStore;
    }

    @Override
    protected void shutdown() {
        this.pmemRawKVStore.shutdown();
    }
}
