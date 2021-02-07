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
     * </p>
     *
     *     cmap : A persistent concurrent engine, backed by a hashmap that allows calling get, put, and remove concurrently from multiple threads
     *     and ensures good scalability. Rest of the methods (e.g. range query methods) are not thread-safe and should not be called from more than one thread.
     **/
    public static final String PMEM_ROOT_PATH = "/mnt/mem/";

    private String             orderedEngine  = "stree";
    private String             hashEngine     = "cmap";
    private int                pmemDataSize   = 1024 * 1024 * 1024;
    private int                pmemMetaSize   = 0;//8 * 1024 * 1024;
    private String             dbPath         = PMEM_ROOT_PATH;
    private boolean            forceCreate    = true;
    private boolean            enableLocker   = false;
    private boolean            lazyInit       = false;

    // for segment snapshot file size
    private int                keysPerSegment = 4096;

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
