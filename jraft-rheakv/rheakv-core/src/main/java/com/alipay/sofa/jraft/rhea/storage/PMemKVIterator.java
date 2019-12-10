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
package com.alipay.sofa.jraft.rhea.storage;

import com.alipay.sofa.jraft.util.BytesUtil;
import lib.util.persistent.PersistentByteArray;
import lib.util.persistent.PersistentSkipListMap;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Jerry Yang
 */
public class PMemKVIterator implements KVIterator {

    private PersistentSkipListMap<PersistentByteArray, PersistentByteArray> map;
    private Iterator<Map.Entry<PersistentByteArray, PersistentByteArray>>   iterator;
    private Map.Entry<PersistentByteArray, PersistentByteArray>             current;

    public PMemKVIterator(PersistentSkipListMap<PersistentByteArray, PersistentByteArray> map) {
        reset(map);
    }

    private void reset(final PersistentSkipListMap<PersistentByteArray, PersistentByteArray> map) {
        this.map = map;
        this.iterator = map.entrySet().iterator();
        next();
    }

    @Override
    public boolean isValid() {
        return this.current != null;
    }

    @Override
    public void seekToFirst() {
        reset(this.map);
    }

    @Override
    public void seekToLast() {
        throw new RuntimeException("Not supported yet");
    }

    @Override
    public void seek(final byte[] target) {
        // TODO : support reverse seek
        // TODO : support Binary Search
        reset(this.map);

        if (!isValid() || BytesUtil.compare(current.getKey().toArray(), target) >= 0) {
            return;
        }
        while (isValid()) {
            if (BytesUtil.compare(current.getKey().toArray(), target) >= 0) {
                break;
            }
            next();
        }
    }

    @Override
    public void seekForPrev(final byte[] target) {
        throw new RuntimeException("Not supported yet");
    }

    @Override
    public void next() {
        if (this.iterator.hasNext()) {
            this.current = this.iterator.next();
        } else {
            this.current = null;
        }
    }

    @Override
    public void prev() {
        throw new RuntimeException("Not supported yet");
    }

    @Override
    public byte[] key() {
        if (!isValid()) {
            return null;
        }
        return current.getKey().toArray();
    }

    @Override
    public byte[] value() {
        if (!isValid()) {
            return null;
        }
        PersistentByteArray v = current.getValue();
        return v == null ? null : v.toArray();
    }

    @Override
    public void close() {
        this.map = null;
        this.iterator = null;
        this.current = null;
    }
}
