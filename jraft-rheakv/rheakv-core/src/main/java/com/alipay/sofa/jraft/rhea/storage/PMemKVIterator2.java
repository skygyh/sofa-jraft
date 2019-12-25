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

import io.pmem.pmemkv.Database;

/**
 * @author Jerry Yang
 */
public class PMemKVIterator2 implements KVIterator {

    private final Database.BytesIterator iterator;

    public PMemKVIterator2(Database db) {
        this.iterator = db.iterator();
    }

    @Override
    public boolean isValid() {
        return this.iterator.isValid();
    }

    @Override
    public void seekToFirst() {
        this.iterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
        this.iterator.seekToLast();
    }

    @Override
    public void seek(final byte[] target) {
        this.iterator.seek(target);
    }

    @Override
    public void seekForPrev(final byte[] target) {
        this.iterator.seekForPrev(target);
    }

    @Override
    public void next() {
        this.iterator.next();
    }

    @Override
    public void prev() {
        this.iterator.prev();
    }

    @Override
    public byte[] key() {
        return this.iterator.key();
    }

    @Override
    public byte[] value() {
        return this.iterator.value();
    }

    @Override
    public void close() {
        this.iterator.close();
    }
}
