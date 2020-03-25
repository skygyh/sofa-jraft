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
package com.alipay.sofa.jraft.rhea.client;

import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.util.BytesUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import static com.alipay.sofa.jraft.rhea.metadata.Region.ANY_REGION_ID;

/**
 *
 * @author jiachun.fjc
 */
public class DefaultRheaIterator implements RheaIterator<KVEntry> {

    private final DefaultRheaKVStore    rheaKVStore;
    private final PlacementDriverClient pdClient;
    private final byte[]                startKey;
    private final byte[]                endKey;
    private final boolean               readOnlySafe;
    private final boolean               returnValue;
    private final int                   bufSize;
    private final Queue<KVEntry>        buf;

    private byte[]                      cursorKey;
    private final long                  regionId;    // specify the regionId to the region only.

    public DefaultRheaIterator(DefaultRheaKVStore rheaKVStore, byte[] startKey, byte[] endKey, int bufSize,
                               boolean readOnlySafe, boolean returnValue) {
        this(rheaKVStore, startKey, endKey, bufSize, readOnlySafe, returnValue, ANY_REGION_ID);
    }

    public DefaultRheaIterator(DefaultRheaKVStore rheaKVStore, byte[] startKey, byte[] endKey, int bufSize,
                               boolean readOnlySafe, boolean returnValue, long regionId) {
        this.rheaKVStore = rheaKVStore;
        this.pdClient = rheaKVStore.getPlacementDriverClient();
        this.startKey = BytesUtil.nullToEmpty(startKey);
        this.endKey = endKey;
        this.bufSize = bufSize;
        this.readOnlySafe = readOnlySafe;
        this.returnValue = returnValue;
        this.buf = new ArrayDeque<>(bufSize);
        this.cursorKey = this.startKey;
        this.regionId = regionId;
    }

    @Override
    public synchronized boolean hasNext() {
        if (this.regionId == ANY_REGION_ID) {
            return hasNextGlobal();
        }
        return hasNextInSingleRegion();
    }

    // when regionId = -1, it indicated the whole cluster is range sharded.
    private boolean hasNextGlobal() {
        if (this.buf.isEmpty()) {
            while (this.endKey == null || BytesUtil.compare(this.cursorKey, this.endKey) < 0) {
                final List<KVEntry> kvEntries = this.rheaKVStore.singleScan(this.cursorKey, this.endKey, this.bufSize,
                    this.readOnlySafe, this.returnValue, this.regionId);
                if (kvEntries.isEmpty()) {
                    // cursorKey jump to next region's startKey
                    this.cursorKey = this.pdClient.findStartKeyOfNextRegion(this.cursorKey, false);
                    if (cursorKey == null) { // current is the last region
                        break;
                    }
                } else {
                    final KVEntry last = kvEntries.get(kvEntries.size() - 1);
                    this.cursorKey = BytesUtil.nextBytes(last.getKey()); // cursorKey++
                    this.buf.addAll(kvEntries);
                    break;
                }
            }
            return !this.buf.isEmpty();
        }
        return true;
    }

    private boolean hasNextInSingleRegion() {
        assert regionId != ANY_REGION_ID;
        if (this.buf.isEmpty()) {
            while (this.endKey == null || BytesUtil.compare(this.cursorKey, this.endKey) < 0) {
                final List<KVEntry> kvEntries = this.rheaKVStore.singleScan(this.cursorKey, this.endKey, this.bufSize,
                    this.readOnlySafe, this.returnValue, this.regionId);
                if (kvEntries.isEmpty()) {
                    break;
                } else {
                    final KVEntry last = kvEntries.get(kvEntries.size() - 1);
                    this.cursorKey = BytesUtil.nextBytes(last.getKey()); // cursorKey++
                    this.buf.addAll(kvEntries);
                    break;
                }
            }
            return !this.buf.isEmpty();
        }
        return true;
    }

    @Override
    public synchronized KVEntry next() {
        if (this.buf.isEmpty()) {
            throw new NoSuchElementException();
        }
        return this.buf.poll();
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public int getBufSize() {
        return bufSize;
    }

    public long getRegionId() {
        return regionId;
    }
}
