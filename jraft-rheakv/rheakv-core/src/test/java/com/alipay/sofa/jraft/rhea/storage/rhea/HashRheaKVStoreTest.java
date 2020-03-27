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
package com.alipay.sofa.jraft.rhea.storage.rhea;

import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.StorageType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeKey;
import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeValue;
import static org.junit.Assert.*;

/**
 * HashRheaKVStoreTest unit test, covering the interfaces with regionId argument of {@link RheaKVStore}
 * when regionId is specified on interface, there may be overlap between regions.
 * But for a given region, the keys inside a region are ordered.
 * So below key mappings are valid, where so called Hash Region.
 *    Region-1 : A B C
 *    Region-2 : B C D
 *
 * @author Jerry Yang
 */
public abstract class HashRheaKVStoreTest extends RheaKVTestCluster {

    public abstract StorageType getStorageType();

    @Before
    public void setup() throws Exception {
        super.start(getStorageType(), true, true);
    }

    @After
    public void tearDown() throws Exception {
        super.shutdown();
    }

    private void checkKeyPresent(final RheaKVStore store, final byte[] key, final byte[] expectedValue,
                                 final long regionId) {
        Region region = store.getPlacementDriverClient().getRegionById(regionId);
        assertEquals(regionId, region.getId());
        assertTrue(store.bContainsKey(regionId, key));
        assertArrayEquals(expectedValue, store.bGet(regionId, key));
    }

    private void checkKeyAbsent(final RheaKVStore store, final byte[] key, final long regionId) {
        Region region = store.getPlacementDriverClient().getRegionById(regionId);
        assertEquals(regionId, region.getId());
        assertFalse(store.bContainsKey(regionId, key));
        assertNull(store.bGet(regionId, key));
    }

    /**
     * Test method: {@link RheaKVStore#get(byte[])}
     */
    private void getTest(RheaKVStore store) {
        byte[] key = makeKey("z_get_test");
        byte[] value = store.bGet(1L, key);
        assertNull(value);
        value = makeValue("z_get_test_value");
        store.bPut(1L, key, value);
        checkKeyPresent(store, key, value, 1L);
        store.bDelete(1L, key);
        checkKeyAbsent(store, key, 1L);

        key = makeKey("a_get_test");
        value = makeValue("a_get_test_value");
        store.bPut(2L, key, value);
        checkKeyPresent(store, key, value, 2L);
        assertArrayEquals(value, store.bGet(2L, key));
    }

    @Test
    public void getByLeaderTest() {
        getTest(getRandomLeaderStore());
    }

    @Test
    public void getByFollowerTest() {
        getTest(getRandomFollowerStore());
    }

    @Test
    public void putByLeaderGetByFollower() {
        byte[] key = makeKey("get_test");
        byte[] value = getRandomLeaderStore().bGet(1L, key);
        assertNull(value);
        value = getRandomLeaderStore().bGet(2L, key);
        assertNull(value);
        value = makeValue("get_test_value");
        getRandomLeaderStore().bPut(1L, key, value);
        assertArrayEquals(value, getRandomFollowerStore().bGet(1L, key));
    }

    // return number of keys in the region
    private int dumpRegion(final RheaKVStore store, final long regionId, String tag) {
        StringBuilder sb = new StringBuilder();
        if (tag != null && !tag.isEmpty()) {
            sb.append(tag).append(" ");
        }
        RheaIterator<KVEntry> it = store.iterator(regionId, "", null, 10);
        if (!it.hasNext()) {
            sb.append("Region-").append(regionId).append(" is empty\n");
            System.err.println(sb.toString());
            return 0;
        }
        int count = 0;
        sb.append("Region-").append(regionId).append(" has:\n");
        while (it.hasNext()) {
            count++;
            KVEntry kvEntry = it.next();
            sb.append("\t[ ").append(BytesUtil.readUtf8(kvEntry.getKey())).append(" => ")
                .append(BytesUtil.readUtf8(kvEntry.getValue())).append(" ]\n");
        }
        System.err.println(sb.toString());
        return count;
    }

    /**
     * Test method: {@link RheaKVStore#multiGet(List)}
     */
    private void multiGetTest(RheaKVStore store) {
        // regions: 1 -> [a, t], 2 -> [j]
        assertEquals(0, dumpRegion(store, 1L, "Initially"));
        assertEquals(0, dumpRegion(store, 2L, "Initially"));
        List<byte[]> keyList1 = Lists.newArrayList();
        List<byte[]> valueList1 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("a_multi_test_key_" + i);
            checkKeyAbsent(store, key, 1L);
            byte[] value = makeValue("a_multi_test_value_" + i);
            keyList1.add(key);
            valueList1.add(value);
            store.bPut(1L, key, value);
        }
        assertEquals(10, dumpRegion(store, 1L, "After bPut 10 keys (a*) in region 1"));
        assertEquals(0, dumpRegion(store, 2L, "After bPut 10 keys (a*) in region 1"));
        List<byte[]> keyList2 = Lists.newArrayList();
        List<byte[]> valueList2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("j_multi_test_key_" + i);
            checkKeyAbsent(store, key, 2L);
            byte[] value = makeValue("j_multi_test_value_" + i);
            keyList2.add(key);
            valueList2.add(value);
            store.bPut(2L, key, value);
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("t_multi_test_key_" + i);
            checkKeyAbsent(store, key, 1L);
            byte[] value = makeValue("t_multi_test_value_" + i);
            keyList1.add(key);
            valueList1.add(value);
            store.bPut(1L, key, value);
        }
        Map<ByteArray, byte[]> mapResult1 = store.bMultiGet(1L, keyList1);
        for (int i = 0; i < valueList1.size(); i++) {
            byte[] key = keyList1.get(i);
            byte[] value = mapResult1.get(ByteArray.wrap(key));
            assertArrayEquals(value, valueList1.get(i));
        }
        Map<ByteArray, byte[]> mapResult2 = store.bMultiGet(2L, keyList2);
        for (int i = 0; i < valueList2.size(); i++) {
            byte[] key = keyList2.get(i);
            byte[] value = mapResult2.get(ByteArray.wrap(key));
            assertArrayEquals(value, valueList2.get(i));
        }

        // check keyList1 is not in region 2
        Map<ByteArray, byte[]> emptyResult1 = store.bMultiGet(2L, keyList1);
        for (int i = 0; i < emptyResult1.size(); i++) {
            byte[] key = keyList1.get(i);
            assertNull(emptyResult1.get(ByteArray.wrap(key)));
        }

        // check keyList2 is not in region 1
        Map<ByteArray, byte[]> emptyResult2 = store.bMultiGet(1L, keyList2);
        for (int i = 0; i < emptyResult2.size(); i++) {
            byte[] key = keyList2.get(i);
            assertNull(emptyResult2.get(ByteArray.wrap(key)));
        }
    }

    @Test
    public void multiGetByLeaderTest() {
        multiGetTest(getRandomLeaderStore());
    }

    @Test
    public void multiGetByFollowerTest() {
        multiGetTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#containsKey(byte[])}
     */
    private void containsKeyTest(RheaKVStore store) {
        byte[] key = makeKey("a_contains_key_test");
        assertFalse(store.bContainsKey(1L, key));
        byte[] value = makeValue("a_contains_key_test_value");
        store.bPut(1L, key, value);
        assertTrue(store.bContainsKey(1L, key));

        key = makeKey("h_contains_key_test");
        assertFalse(store.bContainsKey(2L, key));
        value = makeValue("h_contains_key_test_value");
        store.bPut(2L, key, value);
        assertTrue(store.bContainsKey(2L, key));

        key = makeKey("z_contains_key_test");
        assertFalse(store.bContainsKey(1L, key));
        value = makeValue("z_contains_key_test_value");
        store.bPut(1L, key, value);
        assertTrue(store.bContainsKey(1L, key));
    }

    @Test
    public void containsKeyByLeaderTest() {
        containsKeyTest(getRandomLeaderStore());
    }

    @Test
    public void containsKeyByFollowerTest() {
        containsKeyTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#scan(byte[], byte[])}
     */
    private void scanTest(RheaKVStore store) {
        // regions: 1 -> [0, 8) : scan_test_key_0~8, no_scan_test_key_0~8
        //          2 -> [2, 10) : scan_test_key_2~10, no_scan_test_key_2~10
        List<byte[]> keyList1 = Lists.newArrayList();
        List<byte[]> valueList1 = Lists.newArrayList();
        for (int i = 0; i < 8; i++) {
            byte[] key = makeKey("scan_test_key_" + i);
            assertFalse(store.bContainsKey(1L, key));
            byte[] value = makeValue("scan_test_value_" + i);
            keyList1.add(key);
            valueList1.add(value);
            store.bPut(1L, key, value);
            assertTrue(store.bContainsKey(1L, key));
        }
        for (int i = 0; i < 8; i++) {
            byte[] key = makeKey("no_scan_test_key_" + i);
            assertFalse(store.bContainsKey(1L, key));
            byte[] value = makeValue("no_scan_test_value_" + i);
            store.bPut(1L, key, value);
            assertTrue(store.bContainsKey(1L, key));
        }

        List<byte[]> keyList2 = Lists.newArrayList();
        List<byte[]> valueList2 = Lists.newArrayList();
        for (int i = 2; i < 10; i++) {
            byte[] key = makeKey("scan_test_key_" + i);
            assertFalse(store.bContainsKey(2L, key));
            byte[] value = makeValue("scan_test_value_" + i);
            keyList2.add(key);
            valueList2.add(value);
            store.bPut(2L, key, value);
            assertTrue(store.bContainsKey(2L, key));
        }
        for (int i = 2; i < 10; i++) {
            byte[] key = makeKey("no_scan_test_key_" + i);
            assertFalse(store.bContainsKey(2L, key));
            byte[] value = makeValue("no_scan_test_value_" + i);
            store.bPut(2L, key, value);
            assertTrue(store.bContainsKey(2L, key));
        }

        // bScan(long, byte[], byte[])
        {
            List<KVEntry> entries = store.bScan(1L, makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99));
            assertEquals(entries.size(), keyList1.size());
            for (int i = 0; i < keyList1.size(); i++) {
                assertArrayEquals(keyList1.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList1.get(i), entries.get(i).getValue());
            }
        }

        /// Region 1
        // bScan(long, String, String)
        {
            List<KVEntry> entries = store.bScan(1L, "scan_test_key_", "scan_test_key_" + 99);
            assertEquals(entries.size(), keyList1.size());
            for (int i = 0; i < keyList1.size(); i++) {
                assertArrayEquals(keyList1.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList1.get(i), entries.get(i).getValue());
            }
        }

        // bScan(long, byte[], byte[], Boolean)
        {
            List<KVEntry> entries = store.bScan(1L, makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true);
            assertEquals(entries.size(), keyList1.size());
            for (int i = 0; i < keyList1.size(); i++) {
                assertArrayEquals(keyList1.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList1.get(i), entries.get(i).getValue());
            }
        }

        // bScan(long, String, String, Boolean)
        {
            List<KVEntry> entries = store.bScan(1L, "scan_test_key_", "scan_test_key_" + 99, true);
            assertEquals(entries.size(), keyList1.size());
            for (int i = 0; i < keyList1.size(); i++) {
                assertArrayEquals(keyList1.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList1.get(i), entries.get(i).getValue());
            }
        }

        // bScan(long, byte[], byte[], Boolean, Boolean)
        {
            List<KVEntry> entries = store.bScan(1L, makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true,
                true);
            assertEquals(entries.size(), keyList1.size());
            for (int i = 0; i < keyList1.size(); i++) {
                assertArrayEquals(keyList1.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList1.get(i), entries.get(i).getValue());
            }

            entries = store.bScan(1L, makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true, false);
            assertEquals(entries.size(), keyList1.size());
            for (int i = 0; i < keyList1.size(); i++) {
                assertArrayEquals(keyList1.get(i), entries.get(i).getKey());
                assertNull(entries.get(i).getValue());
            }
        }

        // bScan(long, String, String, Boolean, Boolean)
        {
            List<KVEntry> entries = store.bScan(1L, "scan_test_key_", "scan_test_key_" + 99, true, true);
            assertEquals(entries.size(), keyList1.size());
            for (int i = 0; i < keyList1.size(); i++) {
                assertArrayEquals(keyList1.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList1.get(i), entries.get(i).getValue());
            }

            entries = store.bScan(1L, "scan_test_key_", "scan_test_key_" + 99, true, false);
            assertEquals(entries.size(), keyList1.size());
            for (int i = 0; i < keyList1.size(); i++) {
                assertArrayEquals(keyList1.get(i), entries.get(i).getKey());
                assertNull(entries.get(i).getValue());
            }
        }

        {
            List<KVEntry> entries = store.bScan(1L, null, makeKey("no_scan_test_key_" + 99));
            assertEquals(entries.size(), 8);

            entries = store.bScan(1L, "no_", null);
            assertEquals(entries.size(), 16);
        }

        /// Region 2
        // bScan(long, String, String)
        {
            List<KVEntry> entries = store.bScan(2L, "scan_test_key_", "scan_test_key_" + 99);
            assertEquals(entries.size(), keyList2.size());
            for (int i = 0; i < keyList2.size(); i++) {
                assertArrayEquals(keyList2.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList2.get(i), entries.get(i).getValue());
            }
        }

        // bScan(long, byte[], byte[], Boolean)
        {
            List<KVEntry> entries = store.bScan(2L, makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true);
            assertEquals(entries.size(), keyList2.size());
            for (int i = 0; i < keyList2.size(); i++) {
                assertArrayEquals(keyList2.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList2.get(i), entries.get(i).getValue());
            }
        }

        // bScan(long, String, String, Boolean)
        {
            List<KVEntry> entries = store.bScan(2L, "scan_test_key_", "scan_test_key_" + 99, true);
            assertEquals(entries.size(), keyList2.size());
            for (int i = 0; i < keyList2.size(); i++) {
                assertArrayEquals(keyList2.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList2.get(i), entries.get(i).getValue());
            }
        }

        // bScan(long, byte[], byte[], Boolean, Boolean)
        {
            List<KVEntry> entries = store.bScan(2L, makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true,
                true);
            assertEquals(entries.size(), keyList2.size());
            for (int i = 0; i < keyList2.size(); i++) {
                assertArrayEquals(keyList2.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList2.get(i), entries.get(i).getValue());
            }

            entries = store.bScan(2L, makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), true, false);
            assertEquals(entries.size(), keyList2.size());
            for (int i = 0; i < keyList2.size(); i++) {
                assertArrayEquals(keyList2.get(i), entries.get(i).getKey());
                assertNull(entries.get(i).getValue());
            }
        }

        // bScan(long, String, String, Boolean, Boolean)
        {
            List<KVEntry> entries = store.bScan(2L, "scan_test_key_", "scan_test_key_" + 99, true, true);
            assertEquals(entries.size(), keyList2.size());
            for (int i = 0; i < keyList2.size(); i++) {
                assertArrayEquals(keyList2.get(i), entries.get(i).getKey());
                assertArrayEquals(valueList2.get(i), entries.get(i).getValue());
            }

            entries = store.bScan(2L, "scan_test_key_", "scan_test_key_" + 99, true, false);
            assertEquals(entries.size(), keyList2.size());
            for (int i = 0; i < keyList2.size(); i++) {
                assertArrayEquals(keyList2.get(i), entries.get(i).getKey());
                assertNull(entries.get(i).getValue());
            }
        }

        {
            List<KVEntry> entries = store.bScan(2L, null, makeKey("no_scan_test_key_" + 99));
            assertEquals(entries.size(), 8);

            entries = store.bScan(2L, "no_", null);
            assertEquals(entries.size(), 16);
        }
    }

    @Test
    public void scanByLeaderTest() {
        scanTest(getRandomLeaderStore());
    }

    @Test
    public void scanByFollowerTest() {
        scanTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#scan(byte[], byte[])}
     */
    private void iteratorTest(RheaKVStore store) {
        List<byte[]> keyList1 = Lists.newArrayList();
        List<byte[]> valueList1 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("a_iterator_test_key_" + i);
            checkKeyAbsent(store, key, 1L);
            byte[] value = makeValue("iterator_test_value_" + i);
            keyList1.add(key);
            valueList1.add(value);
            store.bPut(1L, key, value);
        }
        List<byte[]> keyList2 = Lists.newArrayList();
        List<byte[]> valueList2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("h_iterator_test_key_" + i);
            checkKeyAbsent(store, key, 2L);
            byte[] value = makeValue("iterator_scan_test_value_" + i);
            keyList2.add(key);
            valueList2.add(value);
            store.bPut(2L, key, value);
        }

        // Region 1
        // iterator(long, byte[], byte[], int)
        {
            RheaIterator<KVEntry> iterator = store.iterator(1L, makeKey("a_iterator_test_key_"), makeKey("z"), 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList1.get(index), kvEntry.getKey());
                assertArrayEquals(valueList1.get(index), kvEntry.getValue());
            }
        }

        // iterator(long, String, String, int)
        {
            RheaIterator<KVEntry> iterator = store.iterator(1L, "a_iterator_test_key_", "z", 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList1.get(index), kvEntry.getKey());
                assertArrayEquals(valueList1.get(index), kvEntry.getValue());
            }
        }

        // iterator(long, byte[], byte[], int, boolean, boolean)
        {
            RheaIterator<KVEntry> iterator = store.iterator(1L, makeKey("a_iterator_test_key_"), makeKey("z"), 2, true,
                false);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList1.get(index), kvEntry.getKey());
                assertNull(kvEntry.getValue());
            }
        }

        // iterator(long, String, String, int, boolean, boolean)
        {
            RheaIterator<KVEntry> iterator = store.iterator(1L, "a_iterator_test_key_", "z", 2, true, false);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList1.get(index), kvEntry.getKey());
                assertNull(kvEntry.getValue());
            }
        }

        {
            RheaIterator<KVEntry> iterator = store.iterator(1L, "a_iterator_test_key_", "z", 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList1.get(index), kvEntry.getKey());
                assertArrayEquals(valueList1.get(index), kvEntry.getValue());
            }
        }

        {
            RheaIterator<KVEntry> iterator = store.iterator(1L, "a", null, 5);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList1.get(index), kvEntry.getKey());
                assertArrayEquals(valueList1.get(index), kvEntry.getValue());
            }
        }

        // Region 2
        // iterator(long, byte[], byte[], int)
        {
            RheaIterator<KVEntry> iterator = store.iterator(2L, makeKey("h_iterator_test_key_"), makeKey("z"), 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList2.get(index), kvEntry.getKey());
                assertArrayEquals(valueList2.get(index), kvEntry.getValue());
            }
        }

        // iterator(long, String, String, int)
        {
            RheaIterator<KVEntry> iterator = store.iterator(2L, "h_iterator_test_key_", "z", 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList2.get(index), kvEntry.getKey());
                assertArrayEquals(valueList2.get(index), kvEntry.getValue());
            }
        }

        // iterator(long, byte[], byte[], int, boolean, boolean)
        {
            RheaIterator<KVEntry> iterator = store.iterator(2L, makeKey("h_iterator_test_key_"), makeKey("z"), 2, true,
                false);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList2.get(index), kvEntry.getKey());
                assertNull(kvEntry.getValue());
            }
        }

        // iterator(long, String, String, int, boolean, boolean)
        {
            RheaIterator<KVEntry> iterator = store.iterator(2L, "h_iterator_test_key_", "z", 2, true, false);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList2.get(index), kvEntry.getKey());
                assertNull(kvEntry.getValue());
            }
        }

        {
            RheaIterator<KVEntry> iterator = store.iterator(2L, "h_iterator_test_key_", "z", 2);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList2.get(index), kvEntry.getKey());
                assertArrayEquals(valueList2.get(index), kvEntry.getValue());
            }
        }

        {
            RheaIterator<KVEntry> iterator = store.iterator(2L, "h", null, 5);
            int i = 0;
            while (iterator.hasNext()) {
                final int index = i++;
                final KVEntry kvEntry = iterator.next();
                System.err.println("index " + index);
                System.err.println(BytesUtil.readUtf8(kvEntry.getKey()));
                assertArrayEquals(keyList2.get(index), kvEntry.getKey());
                assertArrayEquals(valueList2.get(index), kvEntry.getValue());
            }
        }
    }

    @Test
    public void iteratorByLeaderTest() {
        iteratorTest(getRandomLeaderStore());
    }

    @Test
    public void iteratorByFollowerTest() {
        iteratorTest(getRandomFollowerStore());
    }

    //TODO Fix getSequenceTest
    /**
     * Test method: {@link RheaKVStore#getSequence(byte[], int)}
     */
    private void getSequenceTest(RheaKVStore store) {
        byte[] seqKey = makeKey("seq_test");
        assertEquals(store.bGetLatestSequence(1L, seqKey).longValue(), 0L);
        Sequence sequence = store.bGetSequence(1L, seqKey, 199);
        assertEquals(sequence.getStartValue(), 0);
        assertEquals(sequence.getEndValue(), 199);

        final long latestVal = store.bGetLatestSequence(1L, seqKey);
        assertEquals(latestVal, 199);

        // region 2 has no change
        assertEquals(store.bGetLatestSequence(2L, seqKey).longValue(), 0L);
        // get and update region 2
        store.bGetSequence(2L, seqKey, 15);

        sequence = store.bGetSequence(1L, seqKey, 10);
        assertEquals(sequence.getStartValue(), 199);
        assertEquals(sequence.getEndValue(), 209);

        sequence = store.bGetSequence(1L, seqKey, 10);
        assertEquals(sequence.getStartValue(), 209);
        assertEquals(sequence.getEndValue(), 219);

        store.bResetSequence(1L, seqKey);
        sequence = store.bGetSequence(1L, seqKey, 11);
        assertEquals(sequence.getStartValue(), 0);
        assertEquals(sequence.getEndValue(), 11);

        // region 2
        assertEquals(store.bGetLatestSequence(2L, seqKey).longValue(), 15L);
    }

    @Test
    public void getSequenceByLeaderTest() {
        getSequenceTest(getRandomLeaderStore());
    }

    @Test
    public void getSequenceByFollowerTest() {
        getSequenceTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#put(byte[], byte[])}
     */
    private void putTest(RheaKVStore store) {
        byte[] key = makeKey("put_test");
        checkKeyAbsent(store, key, 1L);
        checkKeyAbsent(store, key, 2L);

        byte[] value = makeValue("put_test_value");
        assertTrue(store.bPut(1L, key, value));
        byte[] newValue = store.bGet(1L, key);
        assertArrayEquals(value, newValue);
    }

    @Test
    public void putByLeaderTest() {
        putTest(getRandomLeaderStore());
    }

    @Test
    public void putByFollowerTest() {
        putTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#put(byte[], byte[])}
     */
    private void putTest(RheaKVStore store, final int keySize, final int valueSize) {
        // regions: 1 -> [null, g), 2 -> [g, null)
        StringBuilder kb1 = new StringBuilder();
        StringBuilder kb2 = new StringBuilder();
        for (int i = 0; i < keySize - 1; i++) {
            kb1.append('a');
            kb2.append('g');
        }
        StringBuilder vb1 = new StringBuilder();
        StringBuilder vb2 = new StringBuilder();
        for (int i = 0; i < valueSize - 1; i++) {
            vb1.append('1');
            vb2.append('2');
        }
        byte[] key1 = makeKey(kb1.toString());
        checkKeyAbsent(store, key1, 1L);
        byte[] value1 = makeValue(vb1.toString());
        assertTrue(store.bPut(1L, key1, value1));
        byte[] newValue = store.bGet(1L, key1);
        assertArrayEquals(value1, newValue);

        byte[] key2 = makeKey(kb2.toString());
        checkKeyAbsent(store, key2, 2L);
        byte[] value2 = makeValue(vb2.toString());
        assertTrue(store.bPut(2L, key2, value2));
        newValue = store.bGet(2L, key2);
        assertArrayEquals(value2, newValue);
    }

    @Test
    public void largePutByLeaderTest() {
        putTest(getRandomLeaderStore(), PMemDBOptions.MAX_KEY_SIZE, PMemDBOptions.MAX_VALUE_SIZE);
    }

    @Test
    public void largePutByFollowerTest() {
        putTest(getRandomFollowerStore(), PMemDBOptions.MAX_KEY_SIZE, PMemDBOptions.MAX_VALUE_SIZE);
    }

    /**
     * Test method: {@link RheaKVStore#getAndPut(byte[], byte[])}
     */
    private void getAndPutTest(RheaKVStore store) {
        byte[] key = makeKey("put_test");
        checkKeyAbsent(store, key, 1L);
        checkKeyAbsent(store, key, 2L);
        byte[] value = store.bGet(1L, key);
        assertNull(value);

        value = makeValue("put_test_value");
        byte[] preVal = store.bGetAndPut(1L, key, value);
        assertNull(preVal);
        byte[] newVal = makeValue("put_test_value_new");
        preVal = store.bGetAndPut(1L, key, newVal);
        assertArrayEquals(value, preVal);
    }

    @Test
    public void getAndPutByLeaderTest() {
        getAndPutTest(getRandomLeaderStore());
    }

    @Test
    public void getAndPutByFollowerTest() {
        getAndPutTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#compareAndPut(byte[], byte[], byte[])}
     */
    private void compareAndPutTest(RheaKVStore store) {
        byte[] key = makeKey("compare_put_test");
        checkKeyAbsent(store, key, 1L);
        checkKeyAbsent(store, key, 2L);
        byte[] value = makeValue("compare_put_test_value");
        store.bPut(2L, key, value);

        byte[] update = makeValue("compare_put_test_update");
        assertTrue(store.bCompareAndPut(2L, key, value, update));
        byte[] newValue = store.bGet(2L, key);
        assertArrayEquals(update, newValue);

        assertFalse(store.bCompareAndPut(2L, key, value, update));
    }

    @Test
    public void compareAndPutByLeaderTest() {
        compareAndPutTest(getRandomLeaderStore());
    }

    @Test
    public void compareAndPutByFollowerTest() {
        compareAndPutTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#merge(String, String)}
     */
    private void mergeTest(RheaKVStore store) {
        String key = "key";
        byte[] value = store.bGet(1L, key);
        assertNull(value);

        store.bMerge(1L, key, "aa");
        store.bMerge(1L, key, "bb");
        store.bMerge(1L, key, "cc");
        value = store.bGet(1L, key);
        assertArrayEquals(value, BytesUtil.writeUtf8("aa,bb,cc"));
    }

    @Test
    public void mergeByLeaderTest() {
        mergeTest(getRandomLeaderStore());
    }

    @Test
    public void mergeByFollowerTest() {
        mergeTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#put(List)}
     */
    private void putListTest(RheaKVStore store) {
        List<KVEntry> entries1 = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            byte[] key = makeKey("batch_put_test_key" + i);
            entries1.add(new KVEntry(1L, key, makeValue("batch_put_test_value" + i)));
        }
        store.bPut(entries1);
        List<KVEntry> entries2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("g_batch_put_test_key" + i);
            entries2.add(new KVEntry(2L, key, makeValue("batch_put_test_value" + i)));
        }
        store.bPut(entries2);
        List<KVEntry> foundList = store.bScan(1L, makeKey("batch_put_test_key"), makeKey("batch_put_test_key" + 99));
        assertEquals(entries1.size(), foundList.size());
        for (int i = 0; i < entries1.size(); i++) {
            assertArrayEquals(entries1.get(i).getKey(), foundList.get(i).getKey());
            assertArrayEquals(entries1.get(i).getValue(), foundList.get(i).getValue());
        }
        foundList = store.bScan(2L, makeKey("g_batch_put_test_key"), makeKey("g_batch_put_test_key" + 99));
        assertEquals(entries2.size(), foundList.size());
        for (int i = 0; i < entries2.size(); i++) {
            assertArrayEquals(entries2.get(i).getKey(), foundList.get(i).getKey());
            assertArrayEquals(entries2.get(i).getValue(), foundList.get(i).getValue());
        }
    }

    @Test
    public void putListByLeaderTest() {
        putListTest(getRandomLeaderStore());
    }

    @Test
    public void putListByFollowerTest1() {
        putListTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#putIfAbsent(byte[], byte[])}
     */
    private void putIfAbsentTest(RheaKVStore store) {
        byte[] key = makeKey("u_put_if_absent_test");
        checkKeyAbsent(store, key, 2L);
        byte[] value = makeValue("put_if_absent_test_value");
        byte[] newValue = makeValue("put_if_absent_test_value_1");
        byte[] oldValue = store.bPutIfAbsent(2L, key, value);
        assertNull(oldValue);
        oldValue = store.bPutIfAbsent(2L, key, newValue);
        assertArrayEquals(value, oldValue);
    }

    @Test
    public void putIfAbsentByLeaderTest() {
        putIfAbsentTest(getRandomLeaderStore());
    }

    @Test
    public void putIfAbsentByFollowerTest() {
        putIfAbsentTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#delete(byte[])}
     */
    private void deleteTest(RheaKVStore store) {
        List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("del_test" + i);
            checkKeyAbsent(store, key, 1L);
            byte[] value = makeValue("del_test_value" + i);
            entries.add(new KVEntry(1L, key, value));
        }
        store.bPut(entries);
        store.bDelete(1L, makeKey("del_test5"));
        List<KVEntry> entries2 = store.bScan(1L, makeKey("del_test"), makeKey("del_test" + 99));
        assertEquals(entries.size() - 1, entries2.size());
        byte[] value = store.bGet(1L, makeKey("del_test5"));
        assertNull(value);
    }

    @Test
    public void deleteByLeaderTest() {
        deleteTest(getRandomLeaderStore());
    }

    @Test
    public void deleteByFollowerTest1() {
        deleteTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#deleteRange(byte[], byte[])}
     */
    private void deleteRangeTest(RheaKVStore store) {
        // region 1 and region 2 are duplicated
        List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("del_range_test" + i);
            checkKeyAbsent(store, key, 1L);
            byte[] value = makeValue("del_range_test_value" + i);
            entries.add(new KVEntry(1L, key, value));
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("del_range_test" + i);
            checkKeyAbsent(store, key, 2L);
            byte[] value = makeValue("del_range_test_value" + i);
            entries.add(new KVEntry(2L, key, value));
        }
        store.bPut(entries);
        dumpRegion(store, 1L, "after put del_range_test0~9 on both regions ");
        dumpRegion(store, 2L, "after put del_range_test0~9 on both regions ");
        // delete region 1 [del_range_test5, del_range_test8)
        store.bDeleteRange(1L, makeKey("del_range_test5"), makeKey("del_range_test8"));
        dumpRegion(store, 1L, "after bDeleteRange del_range_test567 in region1 ");
        dumpRegion(store, 2L, "after bDeleteRange del_range_test567 in region1 ");
        List<KVEntry> entries2 = store.bScan(1L, makeKey("del_range_test"), makeKey("del_range_test" + 99));
        assertEquals(7, entries2.size());
        byte[] value = store.bGet(1L, makeKey("del_range_test5"));
        assertNull(value);
        value = store.bGet(1L, makeKey("del_range_test6"));
        assertNull(value);
        value = store.bGet(1L, makeKey("del_range_test7"));
        assertNull(value);
        value = store.bGet(1L, makeKey("del_range_test8"));
        assertNotNull(value);

        // region 2 has no change
        List<KVEntry> entries3 = store.bScan(2L, makeKey("del_range_test"), makeKey("del_range_test" + 99));
        assertEquals(10, entries3.size());
    }

    @Test
    public void deleteRangeByLeaderTest() {
        deleteRangeTest(getRandomLeaderStore());
    }

    @Test
    public void deleteRangeByFollowerTest() {
        deleteRangeTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#delete(List)}
     */
    private void deleteListTest(RheaKVStore store) {
        List<KVEntry> entries1 = Lists.newArrayList();
        List<byte[]> keys1 = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            byte[] key = makeKey("batch_del_test_key" + i);
            checkKeyAbsent(store, key, 1);
            entries1.add(new KVEntry(1L, key, makeValue("batch_del_test_value" + i)));
            keys1.add(key);
        }
        store.bPut(entries1);
        store.bDelete(1L, keys1);
        List<KVEntry> entries2 = Lists.newArrayList();
        List<byte[]> keys2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("g_batch_del_test_key" + i);
            checkKeyAbsent(store, key, 2L);
            entries2.add(new KVEntry(2L, key, makeValue("batch_del_test_value" + i)));
            keys2.add(key);
        }
        store.bPut(entries2);
        store.bDelete(2L, keys2);
        List<KVEntry> foundList = store.bScan(1L, makeKey("batch_del_test_key"), makeKey("batch_del_test_key" + 99));
        assertEquals(0, foundList.size());
        for (int i = 0; i < keys1.size(); i++) {
            byte[] value = store.bGet(1L, keys1.get(i));
            assertNull(value);
        }
        foundList = store.bScan(2L, makeKey("g_batch_del_test_key"), makeKey("g_batch_put_test_key" + 99));
        assertEquals(0, foundList.size());
        for (int i = 0; i < keys2.size(); i++) {
            byte[] value = store.bGet(2L, keys2.get(i));
            assertNull(value);
        }
    }

    @Test
    public void deleteListByLeaderTest() {
        deleteListTest(getRandomLeaderStore());
    }

    @Test
    public void deleteListByFollowerTest() {
        deleteListTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#batch(List)}
     */
    private void batchCompositeTest(RheaKVStore store) {
        List<KVEntry> entries1 = Lists.newArrayList();
        List<byte[]> keys1 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("batch_composite_test_key" + i);
            checkKeyAbsent(store, key, 1L);
            entries1.add(new KVEntry(key, makeValue("batch_composite_test_value" + i)));
            keys1.add(key);
        }
        store.bPut(entries1);

        List<KVEntry> entries2 = Lists.newArrayList();
        List<byte[]> keys2 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("g_batch_composite_test_key" + i);
            checkKeyAbsent(store, key, 2L);
            entries2.add(new KVEntry(2L, key, makeValue("batch_composite_test_value" + i)));
            keys2.add(key);
        }
        store.bPut(entries2);

        // mixed put and delete in single batch
        List<KVOperation> entries3 = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key1 = keys1.get(i);
            byte[] key2 = keys2.get(i);
            if (i % 2 == 0) {
                entries3.add(new KVOperation(1L, key1, makeValue("batch_composite_test_value" + i * 100), null,
                    KVOperation.PUT));
                entries3.add(new KVOperation(2L, key2, makeValue("batch_composite_test_value" + i * 100), null,
                    KVOperation.PUT));
            } else {
                // delete odd index keys
                entries3.add(new KVOperation(1L, key1, null, null, KVOperation.DELETE));
                entries3.add(new KVOperation(2L, key2, null, null, KVOperation.DELETE));
            }
        }
        store.bBatch(entries3);

        List<KVEntry> foundList1 = store.bScan(1L, makeKey("batch_composite_test_key"),
            makeKey("batch_composite_test_key" + 99));
        assertEquals(5, foundList1.size());
        for (int i = 0; i < keys1.size(); i++) {
            byte[] value = store.bGet(1L, keys1.get(i));
            if (i % 2 == 0) {
                assertNotNull(value);
                assertEquals(0, BytesUtil.compare(value, makeValue("batch_composite_test_value" + i * 100)));
            } else {
                assertNull(value);
            }
        }
        List<KVEntry> foundList2 = store.bScan(2L, makeKey("g_batch_composite_test_key"),
            makeKey("g_batch_composite_test_key" + 99));
        assertEquals(5, foundList2.size());
        for (int i = 0; i < keys2.size(); i++) {
            byte[] value = store.bGet(2L, keys2.get(i));
            if (i % 2 == 0) {
                assertNotNull(value);
                assertEquals(0, BytesUtil.compare(value, makeValue("batch_composite_test_value" + i * 100)));
            } else {
                assertNull(value);
            }
        }
    }

    @Test
    public void batchCompositeByLeaderTest() {
        batchCompositeTest(getRandomLeaderStore());
    }

    @Test
    public void batchCompositeByFollowerTest() {
        batchCompositeTest(getRandomFollowerStore());
    }

    /**
     * Test method: {@link RheaKVStore#getDistributedLock(byte[], long, TimeUnit)}
     */
    private void distributedLockTest(RheaKVStore store) throws InterruptedException {
        // regions: 1 -> [null, g), 2 -> [g, null)
        byte[] lockKey = makeKey("lock_test");
        //checkRegion(store, lockKey, 2);
        final DistributedLock<byte[]> lock = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
        assertNotNull(lock);
        assertTrue(lock.tryLock()); // can lock
        assertTrue(lock.tryLock()); // reentrant lock
        final CountDownLatch latch1 = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            final DistributedLock<byte[]> lock1 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                assertTrue(!lock1.tryLock()); // fail to lock
            } finally {
                latch1.countDown();
            }
        }, "locker1-thread");
        t.setDaemon(true);
        t.start();
        latch1.await();
        final CountDownLatch latch2 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock2 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                // Waiting for the lock to expire and lock successfully
                assertTrue(lock2.tryLock(3100, TimeUnit.MILLISECONDS));
            } finally {
                lock2.unlock();
                latch2.countDown();
            }
        }, "locker2-thread");
        t.setDaemon(true);
        t.start();
        latch2.await();

        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock());
        lock.unlock();
        final CountDownLatch latch3 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock3 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                // The previous lock was not released and the lock failed.
                assertTrue(!lock3.tryLock());
            } finally {
                latch3.countDown();
            }
        }, "locker3-thread");
        t.setDaemon(true);
        t.start();
        latch3.await();

        lock.unlock();
        lock.unlock();
        final CountDownLatch latch4 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock4 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                assertTrue(lock4.tryLock()); // lock success
            } finally {
                lock4.unlock();
                latch4.countDown();
            }
        }, "locker4-thread");
        t.setDaemon(true);
        t.start();
        latch4.await();

        // lock with lease scheduler
        final ScheduledExecutorService watchdog = Executors.newScheduledThreadPool(1);
        final DistributedLock<byte[]> lockWithScheduler = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS,
                watchdog);
        assertTrue(lockWithScheduler.tryLock());
        Thread.sleep(5000);

        final CountDownLatch latch5 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock5 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                // Locking failed because lockWithScheduler is renewing
                assertTrue(!lock5.tryLock());
            } finally {
                latch5.countDown();
            }
        }, "locker5-thread");
        t.setDaemon(true);
        t.start();
        latch5.await();

        lockWithScheduler.unlock();

        final CountDownLatch latch6 = new CountDownLatch(1);
        t = new Thread(() -> {
            final DistributedLock<byte[]> lock6 = store.getDistributedLock(lockKey, 3, TimeUnit.SECONDS);
            try {
                assertTrue(lock6.tryLock()); // lock success
            } finally {
                lock6.unlock();
                latch6.countDown();
            }
        }, "locker6-thread");
        t.setDaemon(true);
        t.start();
        latch6.await();
    }

    //temp disable now
    //@Test
    public void distributedLockByLeaderTest() throws InterruptedException {
        distributedLockTest(getRandomLeaderStore());
    }

    //@Test
    public void distributedLockByFollowerTest() throws InterruptedException {
        distributedLockTest(getRandomFollowerStore());
    }

    private void batchWriteTest(RheaKVStore store) throws ExecutionException, InterruptedException {
        // regions: 1 -> [null, g), 2 -> [g, t), 3 -> [t, null)
        List<CompletableFuture<Boolean>> futures = Lists.newArrayList();
        for (int i = 0; i < 1000; i++) {
            CompletableFuture<Boolean> f = store.put(1L, makeKey("batch" + i), makeValue("batch"));
            futures.add(f);
        }
        FutureGroup<Boolean> futureGroup = new FutureGroup<>(futures);
        CompletableFuture.allOf(futureGroup.toArray()).get();

        futures.clear();
        for (int i = 0; i < 1000; i++) {
            CompletableFuture<Boolean> f = store.delete(1L, makeKey("batch" + i));
            futures.add(f);
        }
        futureGroup = new FutureGroup<>(futures);
        CompletableFuture.allOf(futureGroup.toArray()).get();

        for (int i = 0; i < 1000; i++) {
            byte[] value = store.bGet(1L, makeKey("batch" + i));
            assertNull(value);
        }
    }

    @Test
    public void batchWriteByLeaderTest() throws ExecutionException, InterruptedException {
        batchWriteTest(getRandomLeaderStore());
    }

    @Test
    public void batchWriteByFollowerTest() throws ExecutionException, InterruptedException {
        batchWriteTest(getRandomFollowerStore());
    }

    /*
    @Test
    public void rangeSplitTest() {
        final RheaKVStore store = getRandomLeaderStore();
        final long regionId = 1;
        for (int i = 0; i < 20; i++) {
            store.bPut("a" + i, BytesUtil.writeUtf8("split"));
        }
        final CliOptions opts = new CliOptions();
        opts.setTimeoutMs(30000);
        final RheaKVCliService cliService = RheaKVServiceFactory.createAndInitRheaKVCliService(opts);
        final long newRegionId = 101;
        final String groupId = JRaftHelper.getJRaftGroupId("rhea_test", regionId);
        final Configuration conf = JRaftUtils.getConfiguration("127.0.0.1:18181,127.0.0.1:18182,127.0.0.1:18183");
        final Status st = cliService.rangeSplit(regionId, newRegionId, groupId, conf);
        System.err.println("Status:" + st);
        assertTrue(st.isOk());
        final RheaKVStore newStore = getLeaderStore(101);
        newStore.bPut("f_first_key", BytesUtil.writeUtf8("split_ok"));
        assertArrayEquals(BytesUtil.writeUtf8("split_ok"), newStore.bGet("f_first_key"));
    }
     */

    @Test
    public void restartAllWithLeaderTest() throws Exception {
        RheaKVStore store = getRandomLeaderStore();
        // regions: 1 -> [a, z], 2 -> [h]
        store.bPut(1L, "a_restart_all_with_leader_test", makeValue("a_restart_all_with_leader_test_value"));
        store.bPut(2L, "b_restart_all_with_leader_test", makeValue("b_restart_all_with_leader_test_value"));

        store.resetSequence(1L, "a_restart_all_with_leader_test");
        store.resetSequence(2L, "b_restart_all_with_leader_test");
        assertEquals(0L, store.bGetSequence(1L, "a_restart_all_with_leader_test", 10).getStartValue());
        assertEquals(11L, store.bGetSequence(2L, "b_restart_all_with_leader_test", 11).getEndValue());
        assertEquals(10L, store.bGetLatestSequence(1L, "a_restart_all_with_leader_test").longValue());
        assertEquals(11L, store.bGetLatestSequence(2L, "b_restart_all_with_leader_test").longValue());

        shutdown(false);

        start(getStorageType(), false, true);

        store = getRandomLeaderStore();

        assertArrayEquals(makeValue("a_restart_all_with_leader_test_value"),
            store.bGet(1L, "a_restart_all_with_leader_test"));
        assertArrayEquals(makeValue("b_restart_all_with_leader_test_value"),
            store.bGet(2L, "b_restart_all_with_leader_test"));

        assertEquals(10, store.bGetSequence(1L, "a_restart_all_with_leader_test", 1).getStartValue());
        assertEquals(11, store.bGetSequence(2L, "b_restart_all_with_leader_test", 1).getStartValue());
        assertEquals(11L, store.bGetLatestSequence(1L, "a_restart_all_with_leader_test").longValue());
        assertEquals(12L, store.bGetLatestSequence(2L, "b_restart_all_with_leader_test").longValue());
    }

    @Test
    public void restartAllWithFollowerTest() throws Exception {
        RheaKVStore store = getRandomFollowerStore();
        // regions: 1 -> [a, z], 2 -> [h]
        store.bPut(1L, "a_restart_all_with_follower_test", makeValue("a_restart_all_with_follower_test_value"));
        store.bPut(2L, "h_restart_all_with_follower_test", makeValue("h_restart_all_with_follower_test_value"));
        store.bPut(1L, "z_restart_all_with_follower_test", makeValue("z_restart_all_with_follower_test_value"));

        store.bGetSequence(1L, "a_restart_all_with_follower_seqTest", 10);
        store.bGetSequence(2L, "h_restart_all_with_follower_seqTest", 11);
        store.bGetSequence(1L, "z_restart_all_with_follower_seqTest", 12);

        shutdown(false);

        start(getStorageType(), false, true);

        store = getRandomFollowerStore();

        assertArrayEquals(makeValue("a_restart_all_with_follower_test_value"),
            store.bGet(1L, "a_restart_all_with_follower_test"));
        assertArrayEquals(makeValue("h_restart_all_with_follower_test_value"),
            store.bGet(2L, "h_restart_all_with_follower_test"));
        assertArrayEquals(makeValue("z_restart_all_with_follower_test_value"),
            store.bGet(1L, "z_restart_all_with_follower_test"));

        assertEquals(10, store.bGetSequence(1L, "a_restart_all_with_follower_seqTest", 1).getStartValue());
        assertEquals(11, store.bGetSequence(2L, "h_restart_all_with_follower_seqTest", 1).getStartValue());
        assertEquals(12, store.bGetSequence(1L, "z_restart_all_with_follower_seqTest", 1).getStartValue());
    }

    private void destoryRegionTest(final RheaKVStore store) {
        // duplicated key in two regions
        final String key1 = "a_destroy_region_test";
        final String key2 = "a_destroy_region_test";
        final byte[] value = makeValue("a_destroy_region_test_value");
        assertTrue(store.bPut(1L, key1, value));
        assertTrue(store.bPut(2L, key2, value));
        dumpRegion(store, 1L, "After put key in both regions");
        dumpRegion(store, 2L, "After put key in both regions");
        boolean success = false;
        try {
            success = store.destroyRegion(1L).get();
        } catch (InterruptedException | ExecutionException e) {
            fail("failed to destroy region 1 : " + e.getMessage());
        }
        assertTrue(success);
        byte[] value1 = store.bGet(1L, key1);
        assertNull(value1);

        byte[] value2 = store.bGet(2L, key2);
        assertNotNull(value2);
        assertArrayEquals(value2, value);
        try {
            success = store.destroyRegion(2L).get();
        } catch (InterruptedException | ExecutionException e) {
            fail("failed to destroy region 2 : " + e);
        }
        assertTrue(success);
        value2 = store.bGet(2L, key2);
        assertNull(value2);
    }

    @Test
    public void destroyRegionByLeaderTest() {
        destoryRegionTest(getRandomLeaderStore());
    }

    @Test
    public void destroyRegionByFollowerTest() {
        destoryRegionTest(getRandomFollowerStore());
    }

    private void sealRegionTest(final RheaKVStore store) {
        final String key = "a_seal_region_test";
        final byte[] value = makeValue("seal_region_test_value");
        store.bPut(1L, key, value);
        boolean success = false;
        try {
            success = store.sealRegion(1L).get();
        } catch (InterruptedException | ExecutionException e) {
            fail("failed to seal region 1 : " + e);
        }
        assertTrue(success);
        // update on sealed store would cause fatal error
        //final byte[] update = makeValue("seal_region_test_update");
        //assertFalse(store.bPut(1L, key, update));
        assertArrayEquals(value, store.bGet(1L, key));
    }

    @Test
    public void sealRegionByLeaderTest() {
        sealRegionTest(getRandomLeaderStore());
    }

    @Test
    public void sealRegionByFollowerTest() {
        sealRegionTest(getRandomFollowerStore());
    }
}
