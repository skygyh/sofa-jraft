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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.StoreEngineHelper;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.options.configured.PMemDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.*;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeKey;
import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeValue;
import static org.junit.Assert.*;

/**
 * PMemDB unit test, covering all interfaces in {@link PMemRawKVStore}
 * Clone from MemoryKVStoreTest
 *
 * @author Jerry Yang
 */
public class PMemKVStoreTest extends BaseKVStoreTest {

    private static final String SNAPSHOT_ARCHIVE = "kv.zip";

    @Before
    public void setup() throws Exception {
        super.setup(true);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown(true);
    }

    /**
     * Test method: {@link PMemRawKVStore#get(byte[], KVStoreClosure)}
     */
    @Test
    public void getTest() {
        final byte[] key = makeKey("get_test");
        byte[] value = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertNull(value);

        value = makeValue("get_test_value");
        this.kvStore.put(key, value, null);
        byte[] newValue = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertArrayEquals(value, newValue);
    }

    /**
     * Test method: {@link PMemRawKVStore#multiGet(List, KVStoreClosure)}
     */
    @Test
    public void multiGetTest() {
        final List<byte[]> keyList = Lists.newArrayList();
        final List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("multi_test_key_" + i);
            byte[] value = makeValue("multi_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            this.kvStore.put(key, value, null);
        }
        Map<ByteArray, byte[]> mapResult = new SyncKVStore<Map<ByteArray, byte[]>>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.multiGet(keyList, closure);
            }
        }.apply(this.kvStore);
        for (int i = 0; i < keyList.size(); i++) {
            byte[] key = keyList.get(i);
            assertArrayEquals(mapResult.get(ByteArray.wrap(key)), valueList.get(i));
        }
    }

    /**
     * Test method: {@link PMemRawKVStore#localIterator()}
     */
    @Test
    public void getLocalIteratorTest() {
        final List<byte[]> keyList = Lists.newArrayList();
        final List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("iterator_test_key_" + i);
            byte[] value = makeValue("iterator_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            this.kvStore.put(key, value, null);
        }

        final List<KVEntry> entries = Lists.newArrayList();
        KVIterator it = this.kvStore.localIterator();
        try {
            it.seekToFirst();
            while (it.isValid()) {
                entries.add(new KVEntry(it.key(), it.value()));
                it.next();
            }
        } finally {
            try {
                it.close();
            } catch (Exception ignored) {
                // ignored
            }
        }

        assertEquals(entries.size(), keyList.size());

        for (int i = 0; i < keyList.size(); i++) {
            assertArrayEquals(keyList.get(i), entries.get(i).getKey());
            assertArrayEquals(valueList.get(i), entries.get(i).getValue());
        }

        entries.clear();
        //
        it = this.kvStore.localIterator();
        try {
            it.seekToLast();
            while (it.isValid()) {
                entries.add(new KVEntry(it.key(), it.value()));
                it.prev();
            }
        } finally {
            try {
                it.close();
            } catch (Exception ignored) {
                // ignored
            }
        }
        for (int i = 0; i < keyList.size(); i++) {
            final int cmpIndex = keyList.size() - 1 - i;
            assertArrayEquals(keyList.get(i), entries.get(cmpIndex).getKey());
            assertArrayEquals(valueList.get(i), entries.get(cmpIndex).getValue());
        }

        entries.clear();
        //
        it = this.kvStore.localIterator();
        try {
            it.seek(BytesUtil.writeUtf8("iterator_test_key_5"));
            while (it.isValid()) {
                entries.add(new KVEntry(it.key(), it.value()));
                it.next();
            }
        } finally {
            try {
                it.close();
            } catch (Exception ignored) {
                // ignored
            }
        }
        for (int i = 5; i < keyList.size(); i++) {
            assertArrayEquals(keyList.get(i), entries.get(i - 5).getKey());
            assertArrayEquals(valueList.get(i), entries.get(i - 5).getValue());
        }

        entries.clear();
        //
        it = this.kvStore.localIterator();
        try {
            it.seekForPrev(BytesUtil.writeUtf8("iterator_test_key_5"));
            while (it.isValid()) {
                entries.add(new KVEntry(it.key(), it.value()));
                it.prev();
            }
        } finally {
            try {
                it.close();
            } catch (Exception ignored) {
                // ignored
            }
        }
        for (int i = 0; i < 5; i++) {
            assertArrayEquals(keyList.get(i), entries.get(4 - i).getKey());
            assertArrayEquals(valueList.get(i), entries.get(4 - i).getValue());
        }
    }

    /**
     * Test method: {@link PMemRawKVStore#containsKey(byte[], KVStoreClosure)}
     */
    @Test
    public void containsKeyTest() {
        final byte[] key = makeKey("contains_key_test");
        Boolean isContains = new SyncKVStore<Boolean>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.containsKey(key, closure);
            }
        }.apply(this.kvStore);
        assertFalse(isContains);

        final byte[] value = makeValue("contains_key_test_value");
        this.kvStore.put(key, value, null);
        isContains = new SyncKVStore<Boolean>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.containsKey(key, closure);
            }
        }.apply(this.kvStore);
        assertTrue(isContains);
    }

    /**
     * Test method: {@link PMemRawKVStore#scan(byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void scanTest() {
        final List<byte[]> keyList = Lists.newArrayList();
        final List<byte[]> valueList = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("scan_test_key_" + i);
            byte[] value = makeValue("scan_test_value_" + i);
            keyList.add(key);
            valueList.add(value);
            this.kvStore.put(key, value, null);
        }
        for (int i = 0; i < 10; i++) {
            byte[] key = makeKey("no_scan_test_key_" + i);
            byte[] value = makeValue("no_scan_test_value_" + i);
            this.kvStore.put(key, value, null);
        }
        List<KVEntry> entries = new SyncKVStore<List<KVEntry>>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.scan(makeKey("scan_test_key_"), makeKey("scan_test_key_" + 99), closure);
            }
        }.apply(this.kvStore);
        assertEquals(entries.size(), keyList.size());
        for (int i = 0; i < keyList.size(); i++) {
            assertArrayEquals(keyList.get(i), entries.get(i).getKey());
            assertArrayEquals(valueList.get(i), entries.get(i).getValue());
        }

        entries = new SyncKVStore<List<KVEntry>>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.scan(null, null, closure);
            }
        }.apply(this.kvStore);
        assertEquals(entries.size(), 20);
    }

    /**
     * Test method: {@link PMemRawKVStore#getSequence(byte[], int, KVStoreClosure)}
     */
    @Test
    public void getSequenceTest() throws InterruptedException {
        final byte[] seqKey = makeKey("seq_test");
        Sequence sequence = new SyncKVStore<Sequence>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.getSequence(seqKey, 199, closure);
            }
        }.apply(this.kvStore);
        assertEquals(sequence.getStartValue(), 0);
        assertEquals(sequence.getEndValue(), 199);

        Sequence sequence2 = new SyncKVStore<Sequence>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.getSequence(seqKey, 10, closure);
            }
        }.apply(this.kvStore);
        assertEquals(sequence2.getStartValue(), 199);
        assertEquals(sequence2.getEndValue(), 209);
        this.kvStore.resetSequence(seqKey, null);
        Sequence sequence3 = new SyncKVStore<Sequence>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.getSequence(seqKey, 11, closure);
            }
        }.apply(this.kvStore);
        assertEquals(sequence3.getStartValue(), 0);
        assertEquals(sequence3.getEndValue(), 11);

        // read-only
        Sequence sequence4 = new SyncKVStore<Sequence>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.getSequence(seqKey, 0, closure);
            }
        }.apply(this.kvStore);
        assertEquals(sequence4.getStartValue(), 11);
        assertEquals(sequence4.getEndValue(), 11);

        KVStoreClosure assertFailed = new BaseKVStoreClosure() {
            @Override
            public void run(Status status) {
                assertEquals("Fail to [GET_SEQUENCE], step must >= 0", status.getErrorMsg());
            }
        };
        this.kvStore.getSequence(seqKey, -1, assertFailed);
    }

    /**
     * Test method: {@link PMemRawKVStore#getSafeEndValueForSequence(long, int)}
     */
    @Test
    public void getSafeEndValueForSequenceTest() {
        long startVal = 1;
        assertEquals(2, this.kvStore.getSafeEndValueForSequence(startVal, 1));
        startVal = Long.MAX_VALUE - 1;
        assertEquals(Long.MAX_VALUE, this.kvStore.getSafeEndValueForSequence(startVal, 1));
        assertEquals(Long.MAX_VALUE, this.kvStore.getSafeEndValueForSequence(startVal, 2));
        assertEquals(Long.MAX_VALUE, this.kvStore.getSafeEndValueForSequence(startVal, Integer.MAX_VALUE));
        startVal = Long.MAX_VALUE;
        assertEquals(Long.MAX_VALUE, this.kvStore.getSafeEndValueForSequence(startVal, 0));
        assertEquals(Long.MAX_VALUE, this.kvStore.getSafeEndValueForSequence(startVal, 1));
        assertEquals(Long.MAX_VALUE, this.kvStore.getSafeEndValueForSequence(startVal, 2));
        assertEquals(Long.MAX_VALUE, this.kvStore.getSafeEndValueForSequence(startVal, Integer.MAX_VALUE));
    }

    /**
     * Test method: {@link PMemRawKVStore#put(byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void putTest() {
        final byte[] key = makeKey("put_test");
        TestClosure closure = new TestClosure();
        this.kvStore.get(key, closure);
        byte[] value = (byte[]) closure.getData();
        assertNull(value);

        value = makeValue("put_test_value");
        this.kvStore.put(key, value, null);
        byte[] newValue = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertArrayEquals(value, newValue);
    }

    @Test
    public void putLoopTest() {
        // The max key count is determined by total persistent memory size , MAX_KEY_SIZE and MAX_VALUE_SIZE. check more in PMemDBOptions.java
        final int MaxKeyCount = 100000;
        for (int i = 0; i < MaxKeyCount; i++) {
            final String v = String.valueOf(i);
            this.kvStore.put(makeKey(v), makeValue(v), null);
        }
    }

    /**
     * Test method: {@link PMemRawKVStore#getAndPut(byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void getAndPutTest() {
        final byte[] key = makeKey("put_test");
        TestClosure closure = new TestClosure();
        this.kvStore.get(key, closure);
        byte[] value = (byte[]) closure.getData();
        assertNull(value);

        value = makeValue("put_test_value");
        KVStoreClosure kvStoreClosure = new BaseKVStoreClosure() {
            @Override
            public void run(Status status) {
                assertEquals(status, Status.OK());
            }
        };
        this.kvStore.getAndPut(key, value, kvStoreClosure);
        assertNull(kvStoreClosure.getData());

        this.kvStore.get(key, kvStoreClosure);
        assertArrayEquals(value, (byte[]) kvStoreClosure.getData());

        byte[] newVal = makeValue("put_test_value_new");
        this.kvStore.getAndPut(key, newVal, kvStoreClosure);
        assertArrayEquals(value, (byte[]) kvStoreClosure.getData());
    }

    /**
     * Test method: {@link PMemRawKVStore#compareAndPut(byte[], byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void compareAndPutTest() {
        final byte[] key = makeKey("compare_put_test");
        byte[] value = makeValue("compare_put_test_value");
        this.kvStore.put(key, value, null);

        byte[] update = makeValue("compare_put_test_update");
        KVStoreClosure kvStoreClosure = new BaseKVStoreClosure() {
            @Override
            public void run(Status status) {
                assertEquals(status, Status.OK());
            }
        };
        this.kvStore.compareAndPut(key, value, update, kvStoreClosure);
        assertEquals(kvStoreClosure.getData(), Boolean.TRUE);
        byte[] newValue = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertArrayEquals(update, newValue);

        this.kvStore.compareAndPut(key, value, update, kvStoreClosure);
        assertEquals(kvStoreClosure.getData(), Boolean.FALSE);
    }

    /**
     * Test method: {@link PMemRawKVStore#merge(byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void mergeTest() {
        final byte[] key = BytesUtil.writeUtf8("merge_test");
        final byte[] bytes1 = BytesUtil.writeUtf8("a");
        final byte[] bytes2 = BytesUtil.writeUtf8("b");
        final byte[] bytes3 = BytesUtil.writeUtf8("c");
        final byte[] bytes4 = BytesUtil.writeUtf8("d");
        this.kvStore.put(key, bytes1, null);
        this.kvStore.merge(key, bytes2, null);
        this.kvStore.merge(key, bytes3, null);
        this.kvStore.merge(key, bytes4, null);

        final byte[] val = new SyncKVStore<byte[]>() {

            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
        assertArrayEquals(BytesUtil.writeUtf8("a,b,c,d"), val);
    }

    /**
     * Test method: {@link PMemRawKVStore#put(List, KVStoreClosure)}
     */
    @Test
    public void putListTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            entries.add(new KVEntry(makeKey("batch_put_test_key" + i), makeValue("batch_put_test_value" + i)));
        }
        this.kvStore.put(entries, null);
        final List<KVEntry> entries2 = new SyncKVStore<List<KVEntry>>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.scan(makeKey("batch_put_test_key"), makeKey("batch_put_test_key" + 99), closure);
            }
        }.apply(this.kvStore);
        assertEquals(entries.size(), entries2.size());
        for (int i = 0; i < entries.size(); i++) {
            assertArrayEquals(entries.get(i).getKey(), entries2.get(i).getKey());
            assertArrayEquals(entries.get(i).getValue(), entries2.get(i).getValue());
        }
    }

    /**
     * Test method: {@link PMemRawKVStore#putIfAbsent(byte[], byte[], KVStoreClosure)}
     */
    @Test
    public void putIfAbsent() {
        byte[] key = makeKey("put_if_absent_test");
        byte[] value = makeValue("put_if_absent_test_value");
        TestClosure closure = new TestClosure();
        this.kvStore.putIfAbsent(key, value, closure);
        assertNull(closure.getData());
        this.kvStore.putIfAbsent(key, value, closure);
        assertArrayEquals(value, (byte[]) closure.getData());
    }

    /**
     * Test method: {@link PMemRawKVStore#tryLockWith(byte[], byte[], boolean, DistributedLock.Acquirer, KVStoreClosure)}
     */
    @Test
    public void tryLockWith() throws InterruptedException {
        byte[] lockKey = makeKey("lock_test");
        final DistributedLock<byte[]> lock = new LocalLock(lockKey, 3, TimeUnit.SECONDS, this.kvStore);
        assertNotNull(lock);
        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock(3001, TimeUnit.MILLISECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            final DistributedLock<byte[]> lock2 = new LocalLock(lockKey, 3, TimeUnit.SECONDS, this.kvStore);
            try {
                assertTrue(!lock2.tryLock());
            } finally {
                latch.countDown();
            }
        }, "lock1-thread").start();
        latch.await();
        lock.unlock();
        assertTrue(lock.tryLock());
        lock.unlock();
    }

    /**
     * Test method: {@link PMemRawKVStore#tryLockWith(byte[], byte[], boolean, DistributedLock.Acquirer, KVStoreClosure)}
     */
    @Test
    public void tryLockWithWatchdog() throws InterruptedException {
        byte[] lockKey = makeKey("lock_test_watchdog");
        final ScheduledExecutorService watchdog = Executors.newScheduledThreadPool(1);
        final DistributedLock<byte[]> lock = new LocalLock(lockKey, 3, TimeUnit.SECONDS, this.kvStore, watchdog);
        assertNotNull(lock);
        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock());
        assertTrue(lock.tryLock(3001, TimeUnit.MILLISECONDS));
        Thread.sleep(5000);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            final DistributedLock<byte[]> lock2 = new LocalLock(lockKey, 3, TimeUnit.SECONDS, this.kvStore);
            try {
                // Locking failed because lockWithScheduler is renewing
                assertFalse(lock2.tryLock());
            } finally {
                latch.countDown();
            }
        }, "lock2-thread").start();
        latch.await();

        lock.unlock();
        lock.unlock();
        lock.unlock();

        assertTrue(lock.tryLock());
        lock.unlock();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void deleteTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            entries.add(new KVEntry(makeKey("del_test" + i), makeValue("del_test_value")));
        }
        this.kvStore.put(entries, null);
        this.kvStore.delete(makeKey("del_test5"), null);
        TestClosure closure = new TestClosure();
        this.kvStore.scan(makeKey("del_test"), makeKey("del_test" + 99), closure);
        List<KVEntry> entries2 = (List<KVEntry>) closure.getData();
        assertEquals(entries.size() - 1, entries2.size());
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_test5"), closure);
        byte[] value = (byte[]) closure.getData();
        assertNull(value);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void deleteRangeTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            entries.add(new KVEntry(makeKey("del_range_test" + i), makeValue("del_range_test" + i)));
        }
        this.kvStore.put(entries, null);
        // delete [del_range_test5, del_range_test8)
        this.kvStore.deleteRange(makeKey("del_range_test5"), makeKey("del_range_test8"), null);
        TestClosure closure = new TestClosure();
        this.kvStore.scan(makeKey("del_range_test"), makeKey("del_range_test" + 99), closure);
        final List<KVEntry> entries2 = (List<KVEntry>) closure.getData();
        assertEquals(entries.size() - 3, entries2.size());
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_range_test5"), closure);
        byte[] value = (byte[]) closure.getData();
        assertNull(value);
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_range_test6"), closure);
        value = (byte[]) closure.getData();
        assertNull(value);
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_range_test7"), closure);
        value = (byte[]) closure.getData();
        assertNull(value);
        closure = new TestClosure();
        this.kvStore.get(makeKey("del_range_test8"), closure);
        value = (byte[]) closure.getData();
        assertNotNull(value);
    }

    /**
     * Test method: {@link PMemRawKVStore#delete(List, KVStoreClosure)}
     */
    @SuppressWarnings("unchecked")
    @Test
    public void deleteListTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        final List<byte[]> keys = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final byte[] key = makeKey("batch_del_test" + i);
            entries.add(new KVEntry(key, makeValue("batch_del_test_value")));
            keys.add(key);
        }
        this.kvStore.put(entries, null);
        this.kvStore.delete(keys, null);
        TestClosure closure = new TestClosure();
        this.kvStore.scan(makeKey("batch_del_test"), makeKey("batch_del_test" + 99), closure);
        List<KVEntry> entries2 = (List<KVEntry>) closure.getData();
        assertEquals(0, entries2.size());
        for (int i = 0; i < keys.size(); i++) {
            closure = new TestClosure();
            this.kvStore.get(keys.get(i), closure);
            byte[] value = (byte[]) closure.getData();
            assertNull(value);
        }
    }

    private byte[] get(final byte[] key) {
        return new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(this.kvStore);
    }

    @Test
    public void snapshotTest() throws Exception {
        final File backupDir = new File(this.dbOptions.getDbPath(),"backup");
        if (backupDir.exists()) {
            FileUtils.deleteDirectory(backupDir);
        }
        FileUtils.forceMkdir(backupDir);
        for (int i = 0; i < 100000; i++) {
            final String v = String.valueOf(i);
            this.kvStore.put(makeKey(v), makeValue(v), null);
        }
        for (int i = 0; i < 10000; i++) {
            this.kvStore.getSequence(makeKey((i % 100) + "seq_test"), 10, null);
        }
        final Region region = new Region();
        KVStoreSnapshotFile kvStoreSnapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(this.kvStore);
        final ExecutorService snapshotPool = StoreEngineHelper.createSnapshotExecutor(1, 2);
        final TestSnapshotWriter snapshotWriter = new TestSnapshotWriter(backupDir.getAbsolutePath());
        final CountDownLatch latch = new CountDownLatch(1);
        final Closure done = status -> {
            assertTrue(status.isOk());
            latch.countDown();
        };
        kvStoreSnapshotFile.save(snapshotWriter, region, done, snapshotPool);
        latch.await();
        final LocalFileMeta meta = (LocalFileMeta) snapshotWriter.getFileMeta(SNAPSHOT_ARCHIVE);
        assertNotNull(meta);

        assertNotNull(get(makeKey("1")));

        this.kvStore.put(makeKey("100001"), makeValue("100001"), null);
        assertNotNull(get(makeKey("100001")));

        this.kvStore.shutdown();
        this.kvStore = new PMemRawKVStore();
        this.kvStore.init(this.dbOptions);

        assertNull(get(makeKey("1")));

        final TestSnapshotReader snapshotReader = new TestSnapshotReader(snapshotWriter.metaTable, backupDir.getAbsolutePath());
        kvStoreSnapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(this.kvStore);
        final boolean ret = kvStoreSnapshotFile.load(snapshotReader, region);
        assertTrue(ret);


        for (int i = 0; i < 100000; i++) {
            final String v = String.valueOf(i);
            assertArrayEquals(makeValue(v), get(makeKey(v)));
        }

        // Key[100001] is put after the snapshot, so key[100001] should not exist
        assertNull(get(makeKey("100001")));

        FileUtils.deleteDirectory(backupDir);
        ExecutorServiceHelper.shutdownAndAwaitTermination(snapshotPool);
    }

    @Test
    public void multiGroupSnapshotTest() throws Exception {
        final File backupDir = new File(this.dbOptions.getDbPath(), "multi-backup");
        if (backupDir.exists()) {
            FileUtils.deleteDirectory(backupDir);
        }

        final List<Region> regions = Lists.newArrayList();
        regions.add(new Region(1, makeKey("0"), makeKey("1"), null, null));
        regions.add(new Region(2, makeKey("1"), makeKey("2"), null, null));
        regions.add(new Region(3, makeKey("2"), makeKey("3"), null, null));
        regions.add(new Region(4, makeKey("3"), makeKey("4"), null, null));
        regions.add(new Region(5, makeKey("4"), makeKey("5"), null, null));

        for (int i = 0; i < 5; i++) {
            final String v = String.valueOf(i);
            this.kvStore.put(makeKey(v), makeValue(v), null);
        }
        for (int i = 0; i < 5; i++) {
            this.kvStore.getSequence(makeKey(i + "_seq_test"), 10, null);
        }

        KVStoreSnapshotFile kvStoreSnapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(this.kvStore);
        final ExecutorService snapshotPool = StoreEngineHelper.createSnapshotExecutor(1, 2);
        final List<TestSnapshotWriter> writers = Lists.newArrayList();
        for (int i = 0; i < 4; i++) {
            final Path p = Paths.get(backupDir.getAbsolutePath(), String.valueOf(i));
            final TestSnapshotWriter snapshotWriter = new TestSnapshotWriter(p.toString());
            writers.add(snapshotWriter);
            final CountDownLatch latch = new CountDownLatch(1);
            final Closure done = status -> {
                assertTrue(status.isOk());
                latch.countDown();
            };
            kvStoreSnapshotFile.save(snapshotWriter, regions.get(i), done, snapshotPool);
            latch.await();
            final LocalFileMeta meta = (LocalFileMeta) snapshotWriter.getFileMeta(SNAPSHOT_ARCHIVE);
            assertNotNull(meta);
        }

        this.kvStore.shutdown();
        this.kvStore = new PMemRawKVStore();
        //this.dbOptions.setForceCreate(0);
        this.kvStore.init(this.dbOptions);

        kvStoreSnapshotFile = KVStoreSnapshotFileFactory.getKVStoreSnapshotFile(this.kvStore);
        for (int i = 0; i < 4; i++) {
            final Path p = Paths.get(backupDir.getAbsolutePath(), String.valueOf(i));
            final TestSnapshotReader snapshotReader = new TestSnapshotReader(writers.get(i).metaTable, p.toString());
            final boolean ret = kvStoreSnapshotFile.load(snapshotReader, regions.get(i));
            assertTrue(ret);
        }

        for (int i = 0; i < 4; i++) {
            final String v = String.valueOf(i);
            final byte[] seqKey = makeKey(i + "_seq_test");
            assertArrayEquals(makeValue(v), get(makeKey(v)));
            final Sequence sequence = new SyncKVStore<Sequence>() {
                @Override
                public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                    kvStore.getSequence(seqKey, 10, closure);
                }
            }.apply(this.kvStore);
            assertEquals(10L, sequence.getStartValue());
            assertEquals(20L, sequence.getEndValue());
        }

        assertNull(get(makeKey("5")));
        final Sequence sequence = new SyncKVStore<Sequence>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.getSequence(makeKey("4_seq_test"), 10, closure);
            }
        }.apply(this.kvStore);
        assertEquals(0L, sequence.getStartValue());

        FileUtils.deleteDirectory(backupDir);
        ExecutorServiceHelper.shutdownAndAwaitTermination(snapshotPool);
    }

    @Test
    public void getApproximateKeysInRangeTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10000; i++) {
            entries.add(new KVEntry(makeKey("approximate_test" + i), makeValue("approximate_test_value")));
        }
        this.kvStore.put(entries, null);

        // this is tricky range here, boundary is exclusive.
        long approximateKeys = this.kvStore.getApproximateKeysInRange(makeKey("approximate_test" + 9998), null);
        assertEquals(1, approximateKeys);
        approximateKeys = this.kvStore.getApproximateKeysInRange(null, makeKey("approximate_test" + 9999));
        assertEquals(10000, approximateKeys, 1);
        approximateKeys = this.kvStore.getApproximateKeysInRange(makeKey("approximate_test" + 9990),
            makeKey("approximate_test" + 9999));
        assertEquals(10, approximateKeys, 1);
    }

    @Test
    public void jumpOverTest() {
        final List<KVEntry> entries = Lists.newArrayList();
        for (int i = 0; i < 10000; i++) {
            entries.add(new KVEntry(makeKey("approximate_test" + i), makeValue("approximate_test_value")));
        }
        this.kvStore.put(entries, null);
        final byte[] endKey = this.kvStore.jumpOver(makeKey("approximate_test0000"), 1000);
        final long approximateKeys = this.kvStore.getApproximateKeysInRange(makeKey("approximate_test0000"), endKey);
        assertEquals(1000, approximateKeys, 10);
    }

    @Test
    public void initFencingTokenTest() throws Exception {
        final Method getNextFencingMethod = PMemRawKVStore.class.getDeclaredMethod("getNextFencingToken", byte[].class);
        getNextFencingMethod.setAccessible(true);
        final List<byte[]> parentKeys = Lists.newArrayList();
        parentKeys.add(null); // startKey == null
        parentKeys.add(BytesUtil.writeUtf8("parent"));
        for (int i = 0; i < 2; i++) {
            final byte[] parentKey = parentKeys.get(i);
            final byte[] childKey = BytesUtil.writeUtf8("child");
            assertEquals(1L, getNextFencingMethod.invoke(this.kvStore, (Object) parentKey));
            assertEquals(2L, getNextFencingMethod.invoke(this.kvStore, (Object) parentKey));
            this.kvStore.initFencingToken(parentKey, childKey);
            assertEquals(3L, getNextFencingMethod.invoke(this.kvStore, (Object) childKey));
            assertEquals(4L, getNextFencingMethod.invoke(this.kvStore, (Object) childKey));
        }
    }

    @Test
    public void sealAndDestroyTest() {
        final long id = 1L;
        // create delicate store
        final Path testDbPath = Paths.get(PMemDBOptions.PMEM_ROOT_PATH, "db_PMemKVStoreTest_sealAndDestroyTest",
            "Region" + id);
        PMemRawKVStore store = new PMemRawKVStore(id, testDbPath.toString());
        store.init(PMemDBOptionsConfigured.newConfigured().withPmemDataSize(32 * 1024 * 1024)
            .withPmemMetaSize(8 * 1024 * 1024).withDbPath(testDbPath.toString()).withForceCreate(true).config());
        Assert.assertTrue(Files.exists(testDbPath));

        final byte[] key = makeKey("seal_and_destory_store_key");
        final byte[] value = makeValue("seal_and_destory_store_value");
        store.put(key, value, null);
        // seal the store
        boolean success = new SyncKVStore<Boolean>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.seal(id, closure);
            }
        }.apply(store);
        Assert.assertTrue(success);
        byte[] newValue = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(store);
        assertArrayEquals(value, newValue);

        //destroy the store
        success = new SyncKVStore<Boolean>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.destroy(id, closure);
            }
        }.apply(store);
        Assert.assertTrue(success);

        byte[] nullValue = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(store);
        assertNull(nullValue);

        store.shutdown();
    }

    private boolean isEmptyDir(final Path path) {
        if (!Files.isDirectory(path)) {
            return false;
        }
        String[] files = path.toFile().list();
        return files == null || files.length == 0;
    }

    @Test
    public void restartTest() {
        final long id = 1L;
        final Path testDbPath = Paths
            .get(PMemDBOptions.PMEM_ROOT_PATH, "db_PMemKVStoreTest_restartTest", "Region" + id);
        // create delicate store
        PMemRawKVStore store = new PMemRawKVStore(id, testDbPath.toString());
        store.init(PMemDBOptionsConfigured.newConfigured().withPmemDataSize(32 * 1024 * 1024)
            .withPmemMetaSize(8 * 1024 * 1024).withDbPath(testDbPath.toString()).withForceCreate(true).config());
        Assert.assertTrue(Files.exists(testDbPath));

        final byte[] key = makeKey("restart_store_key");
        final byte[] value = makeValue("restart_store_value");
        store.put(key, value, null);
        byte[] actualValue1 = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(store);
        assertArrayEquals(value, actualValue1);

        //shutdown and restart the store
        store.shutdown();
        Assert.assertFalse(store.isOpen());
        store.init(PMemDBOptionsConfigured.newConfigured().withPmemDataSize(32 * 1024 * 1024)
            .withPmemMetaSize(8 * 1024 * 1024).withDbPath(testDbPath.toString()).withForceCreate(false) // NOT RECREATE!
            .config());
        byte[] actualValue2 = new SyncKVStore<byte[]>() {
            @Override
            public void execute(RawKVStore kvStore, KVStoreClosure closure) {
                kvStore.get(key, closure);
            }
        }.apply(store);
        assertArrayEquals(value, actualValue2);
        // check the db path is non empty
        Assert.assertFalse(isEmptyDir(testDbPath));

        store.shutdown();
    }
}
