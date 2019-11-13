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

/**
 * @author Jerry Yang
 */
public class KVCompositeEntry extends KVEntry {

    private boolean isDelete; // delete or udpate.

    public KVCompositeEntry(byte[] key) {
        super(key, null);
        this.isDelete = true;
    }

    public KVCompositeEntry(byte[] key, byte[] value, boolean isDelete) {
        super(key, value);
        this.isDelete = isDelete;
    }

    public boolean isDelete() {
        return isDelete;
    }

    @Override
    public String toString() {
        return "KVCompositeEntry{" + super.toString() + " delete=" + this.isDelete + "}";
    }
}
