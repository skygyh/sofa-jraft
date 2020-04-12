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
package com.alipay.sofa.jraft.rhea.options.configured;

import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.util.Configured;

/**
 *
 * @author Jerry Yang
 */
public final class PMemDBOptionsConfigured implements Configured<PMemDBOptions> {

    private final PMemDBOptions opts;

    public static PMemDBOptionsConfigured newConfigured() {
        return new PMemDBOptionsConfigured(new PMemDBOptions());
    }

    public PMemDBOptionsConfigured withOrderedEngine(final String orderedEngine) {
        this.opts.setOrderedEngine(orderedEngine);
        return this;
    }

    public PMemDBOptionsConfigured withHashEngine(final String hashEngine) {
        this.opts.setHashEngine(hashEngine);
        return this;
    }

    public PMemDBOptionsConfigured withPmemDataSize(final int pmemDataSize) {
        this.opts.setPmemDataSize(pmemDataSize);
        return this;
    }

    public PMemDBOptionsConfigured withPmemMetaSize(final int pmemMetaSize) {
        this.opts.setPmemMetaSize(pmemMetaSize);
        return this;
    }

    public PMemDBOptionsConfigured withDbPath(final String dbPath) {
        this.opts.setDbPath(dbPath);
        return this;
    }

    public PMemDBOptionsConfigured withForceCreate(final boolean forceCreate) {
        this.opts.setForceCreate(forceCreate);
        return this;
    }

    public PMemDBOptionsConfigured withEnableLocker(final boolean enableLocker) {
        this.opts.setEnableLocker(enableLocker);
        return this;
    }

    public PMemDBOptionsConfigured withLazyInit(final boolean lazyInit) {
        this.opts.setLazyInit(lazyInit);
        return this;
    }

    @Override
    public PMemDBOptions config() {
        return this.opts;
    }

    private PMemDBOptionsConfigured(PMemDBOptions opts) {
        this.opts = opts;
    }
}
