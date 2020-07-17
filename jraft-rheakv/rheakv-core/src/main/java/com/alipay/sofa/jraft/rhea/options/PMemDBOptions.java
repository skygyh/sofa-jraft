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
package com.alipay.sofa.jraft.rhea.options;

/**
 *
 * @author Jerry Yang
 */
public class PMemDBOptions {
    /**
     * Currently, some of the engines are thread safe while others not.
     * <p>
     *     A sorted persitent engine is required as we need. Hence we choose stree engine for now.
     *     The 'stree' is a persistent, single-threaded and sorted engine, backed by a B+ tree.
     *     Sorted persistent B+ tree, thread unsafe.
     *     Please check more on https://github.com/pmem/pmemkv
     *
     *     The 'stree' has size limit on key size and value size, for now hardcoded. check code in stree.h in pmemkv
     *     It is important that MAX_KEY_SIZE and MAX_VALUE_SIZE must be properly set up. Those values
     *     together with pmemory size (pmemDataSize) determine total key count capacity such stree can hold.
     *     The approximate estimation of max key count is :
     *     (PMEM_DATA_SIZE - RESERVED_META_SIZE) / 2 / (MAX_KEY_SIZE + MAX_KEY_SIZE) * RATIO, where RATIO could be 0.9
     * </p>
     *
     *     cmap : A persistent concurrent engine, backed by a hashmap that allows calling get, put, and remove concurrently from multiple threads
     *     and ensures good scalability. Rest of the methods (e.g. range query methods) are not thread-safe and should not be called from more than one thread.
     **/
    public static final String PMEM_ROOT_PATH = "/mnt/mem/";
    // The below MAX_[KEY|VALUE]_SIZE is from stree.h in pmemkv
    public static final int    MAX_KEY_SIZE   = 2048;
    public static final int    MAX_VALUE_SIZE = 2048;

    private String             orderedEngine  = "stree";
    private String             hashEngine     = "cmap";
    private int                pmemDataSize   = 1024 * 1024 * 1024;
    private int                pmemMetaSize   = 8 * 1024 * 1024;
    private String             dbPath         = PMEM_ROOT_PATH;
    private boolean            forceCreate    = true;
    private boolean            enableLocker   = false;
    private boolean            lazyInit       = false;

    // for segment snapshot file size
    private int                keysPerSegment = 4096;

    // rough estimation
    public int approximateKeyCountLimit() {
        return (pmemDataSize - 4 * 1024 * 1024) / 2 / (MAX_KEY_SIZE + MAX_VALUE_SIZE) * 9 / 10;
    }

    public String getOrderedEngine() {
        return orderedEngine;
    }

    public void setOrderedEngine(String orderedEngine) {
        this.orderedEngine = orderedEngine;
    }

    public String getHashEngine() {
        return hashEngine;
    }

    public void setHashEngine(String hashEngine) {
        this.hashEngine = hashEngine;
    }

    public int getPmemDataSize() {
        return pmemDataSize;
    }

    public void setPmemDataSize(int pmemSize) {
        this.pmemDataSize = pmemSize;
    }

    public int getPmemMetaSize() {
        return pmemMetaSize;
    }

    public void setPmemMetaSize(int pmemMetaSize) {
        this.pmemMetaSize = pmemMetaSize;
    }

    public String getDbPath() {
        return dbPath;
    }

    public void setDbPath(String fullPath) {
        this.dbPath = fullPath;
    }

    public boolean getForceCreate() {
        return forceCreate;
    }

    public void setForceCreate(boolean forceCreate) {
        this.forceCreate = forceCreate;
    }

    public boolean getEnableLocker() {
        return enableLocker;
    }

    public void setEnableLocker(boolean enableLocker) {
        this.enableLocker = enableLocker;
    }

    public boolean getLazyInit() {
        return lazyInit;
    }

    public void setLazyInit(boolean lazyInit) {
        this.lazyInit = lazyInit;
    }

    public int getKeysPerSegment() {
        return keysPerSegment;
    }

    public void setKeysPerSegment(int keysPerSegment) {
        this.keysPerSegment = keysPerSegment;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("PMemDBOptions{").append("orderedEngine=").append(orderedEngine).append(',')
            .append("hashEngine=").append(hashEngine).append(',').append("pmemDataSize=").append(pmemDataSize)
            .append(',').append("pmemMetaSize=").append(pmemMetaSize).append(',').append("parentPath=").append(dbPath)
            .append(',').append("forceCreate=").append(forceCreate).append(',').append("enableLocker=")
            .append(enableLocker).append(',').append("keysPerSegment=").append(keysPerSegment).append('}').toString();
    }
}
