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
package com.alipay.sofa.jraft.rhea.storage.pmemdb;

import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.options.configured.PMemDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.PMemRawKVStore;

import java.nio.file.Paths;

public class BaseKVStoreTest {

    protected PMemRawKVStore kvStore;
    protected PMemDBOptions  dbOptions;

    protected void setup() throws Exception {
        this.kvStore = new PMemRawKVStore();
        this.dbOptions = PMemDBOptionsConfigured.newConfigured().withPmemDataSize(512 * 1024 * 1024)
            .withPmemMetaSize(64 * 1024 * 1024)
            .withDbPath(Paths.get(PMemDBOptions.PMEM_ROOT_PATH, "db_PMemKVStoreTest").toString()).withForceCreate(1)
            .config();
        this.kvStore.init(this.dbOptions);
    }

    protected void tearDown() throws Exception {
        this.kvStore.shutdown();
    }
}
