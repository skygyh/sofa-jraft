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

import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.*;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;
import lib.util.persistent.*;
import lib.util.persistent.spi.PersistentMemoryProvider;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Jerry Yang
 * TODO :
 * 1. support customized comparator. (beware dump also need change accordingly)
 * 2. improve Iterator.
 * 3. use more persistent data structure
 */
public class PMemRawKVStore2 extends BatchRawKVStore<PMemDBOptions> {
    private static final Logger                                                 LOG          = LoggerFactory
                                                                                                 .getLogger(PMemRawKVStore2.class);
    private static final byte                                                   DELIMITER    = (byte) ',';

    static {
        PersistentMemoryProvider.getDefaultProvider().getHeap().open();
    }

    private final ReadWriteLock                                                 snapshotLock = new ReentrantReadWriteLock();
    private final Serializer                                                    serializer   = Serializers.getDefault();

    private final long                                                          regionId;
    private boolean                                                             writable     = true;
    private PersistentFPTree2<PMemDecoratedKey, PersistentImmutableByteArray>   defaultDB;
    private PersistentSIHashMap<PMemDecoratedKey, PersistentLong>               sequenceDB;                                        // key -> sequence long
    private PersistentSIHashMap<PMemDecoratedKey, PersistentLong>               fencingKeyDB;                                      // key -> fencing id long
    private PersistentSIHashMap<PMemDecoratedKey, PersistentImmutableByteArray> lockerDB;                                          // key -> DistributedLock.Owner
    private volatile PMemDBOptions                                              opts;

    public PMemRawKVStore2() {
        this(-1L);
    }

    public PMemRawKVStore2(final long regionId) {
        super();
        this.regionId = regionId;
    }

    @SuppressWarnings("unchecked")
    private static PersistentFPTree2<PMemDecoratedKey, PersistentImmutableByteArray> createSortedMap(String dbName,
                                                                                                     int size,
                                                                                                     boolean forceCreate) {
        String id = "persistent_" + dbName;
        PersistentFPTree2<PMemDecoratedKey, PersistentImmutableByteArray> map = ObjectDirectory.get(id,
            PersistentFPTree2.class);
        if (map == null) {
            map = new PersistentFPTree2<>(8, 64);
            ObjectDirectory.put(id, map);
            LOG.info("created sorted map {} {}", id, map.getClass().getSimpleName());
        } else if (forceCreate) {
            map.clear();
            LOG.info("cleared sorted map {} {}", id, map.getClass().getSimpleName());
        } else {
            LOG.info("loaded sorted map {} {}", id, map.getClass().getSimpleName());
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static <T extends AnyPersistent> PersistentSIHashMap<PMemDecoratedKey, T> createHashMap(String dbName,
                                                                                                    int size,
                                                                                                    boolean forceCreate) {
        String id = "persistent_" + dbName;
        PersistentSIHashMap<PMemDecoratedKey, T> map = ObjectDirectory.get(id, PersistentSIHashMap.class);
        if (map == null) {
            map = new PersistentSIHashMap<>();
            ObjectDirectory.put(id, map);
            LOG.info("created hash map {} {}", id, map.getClass().getSimpleName());
        } else if (forceCreate) {
            map.clear();
            LOG.info("cleared hash map {} {}", id, map.getClass().getSimpleName());
        } else {
            LOG.info("loaded hash map {} {}", id, map.getClass().getSimpleName());
        }
        return map;
    }

    private static boolean hasEnoughSpace(PMemDBOptions opts) {
        File f = new File(PMemDBOptions.PMEM_ROOT_PATH);
        long pmemFreeSpace = f.getFreeSpace();
        return pmemFreeSpace > (opts.getPmemDataSize() + opts.getPmemMetaSize() * 3);
    }

    private static String generateConf(final String engine, final String parentPath, String tagName,
                                       final int pmemSize, final int forceCreate) {
        boolean isVolatile = engine.equals("vsmap") || engine.equals("vcmap");
        StringBuilder confStr = new StringBuilder();
        Path fullPath = Paths.get(parentPath, tagName);
        confStr.append("{\"path\":\"").append(fullPath.toString()).append("\", \"size\":").append(pmemSize);
        if (!isVolatile) {
            confStr.append(", \"force_create\":").append(forceCreate);
        }
        confStr.append("}");

        if (forceCreate > 0) {
            if (Files.exists(fullPath)) {
                if (!isVolatile) {
                    try {
                        Files.delete(fullPath);
                        LOG.info("Successfully cleaned {}", fullPath.toString());
                    } catch (IOException ioe) {
                        LOG.error("Failed to clean {} : {}", fullPath.toString(), ioe.getMessage());
                    }
                }
            } else {
                try {
                    if (isVolatile) {
                        Files.createDirectory(fullPath);
                    } else {
                        if (Files.notExists(Paths.get(parentPath))) {
                            Files.createDirectory(Paths.get(parentPath));
                        }
                    }
                } catch (IOException ioe) {
                    LOG.error("Failed to create {} : {}", fullPath.toString(), ioe.getMessage());
                }
            }
        }
        LOG.info("Generated conf : {}", confStr);
        return confStr.toString();
    }

    static Map<ByteArray, Long> subRangeMap(final PersistentSIHashMap<PMemDecoratedKey, PersistentLong> input,
                                            final Region region) {
        final Map<ByteArray, Long> output = new HashMap<>();
        if (RegionHelper.isSingleGroup(region)) {
            for (Map.Entry<PMemDecoratedKey, PersistentLong> e : input.entrySet()) {
                output.put(ByteArray.wrap(e.getKey().toArray()), e.getValue().longValue());
            }
            return output;
        }

        final Iterator<Map.Entry<PMemDecoratedKey, PersistentLong>> it = input.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<PMemDecoratedKey, PersistentLong> e = it.next();
            byte[] key = e.getKey().toArray();
            long value = e.getValue().longValue();
            if (RegionHelper.isKeyInRegion(key, region)) {
                output.put(ByteArray.wrap(key), value);
            }
        }
        return output;
    }

    // Use by Test only
    @Override
    public boolean init(final PMemDBOptions opts) {
        this.opts = opts;

        Requires.requireTrue(hasEnoughSpace(opts), "No enough space for Persistent Memory on "
                                                   + PMemDBOptions.PMEM_ROOT_PATH);
        this.defaultDB = createSortedMap("defaultDB", opts.getPmemDataSize(), opts.getForceCreate());
        this.sequenceDB = createHashMap("sequenceDB", opts.getPmemMetaSize(), opts.getForceCreate());
        this.fencingKeyDB = createHashMap("fencingKeyDB", opts.getPmemMetaSize(), opts.getForceCreate());
        this.lockerDB = createHashMap("lockerDB", opts.getPmemMetaSize(), opts.getForceCreate());
        LOG.info("[PMemRawKVStore2] start successfully, options: {}.", opts);
        return true;
    }

    @Override
    public void shutdown() {
        if (this.defaultDB != null) {
            this.defaultDB.clear();
        }
        if (this.sequenceDB != null) {
            this.sequenceDB.clear();
        }
        if (this.fencingKeyDB != null) {
            this.fencingKeyDB.clear();
        }
        if (this.lockerDB != null) {
            this.lockerDB.clear();
        }
        LOG.info("[PMemRawKVStore2] shutdown successfully");
    }

    @Override
    public KVIterator localIterator() {
        return new PMemKVIterator(this.defaultDB);
    }

    @Override
    public boolean isOpen() {
        return this.defaultDB != null;
    }

    @Override
    public void size(final KVStoreClosure closure) {
        ////final Timer.Context timeCtx = getTimeContext("SIZE");
        try {
            setSuccess(closure, this.defaultDB.size());
        } catch (final Exception e) {
            LOG.error("Fail to [SIZE], key: {}.", StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [SIZE]");
        } finally {
            ////timeCtx.stop();
        }
    }

    @Override
    public void get(final byte[] key, @SuppressWarnings("unused") final boolean readOnlySafe,
                    final KVStoreClosure closure) {
        Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
        ////final Timer.Context timeCtx = getTimeContext("GET");
        try {
            final PersistentImmutableByteArray v = this.defaultDB.get(new PMemDecoratedKey(key));
            final byte[] value = (v == null) ? null : v.toArray();
            setSuccess(closure, value);
        } catch (final Exception e) {
            LOG.error("Fail to [GET], key: [{}], {}.", BytesUtil.readUtf8(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [GET]");
        } finally {
            ////timeCtx.stop();
        }
    }

    @Override
    public void multiGet(final List<byte[]> keys, @SuppressWarnings("unused") final boolean readOnlySafe,
                         final KVStoreClosure closure) {
        ////final Timer.Context timeCtx = getTimeContext("MULTI_GET");
        try {
            final Map<ByteArray, byte[]> resultMap = Maps.newHashMap();
            for (final byte[] key : keys) {
                Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
                final PersistentImmutableByteArray v = this.defaultDB.get(new PMemDecoratedKey(key));
                resultMap.put(ByteArray.wrap(key), v == null ? null : v.toArray());
            }
            setSuccess(closure, resultMap);
        } catch (final Exception e) {
            LOG.error("Fail to [MULTI_GET], key size: [{}], {}.", keys.size(), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [MULTI_GET]");
        } finally {
            ////timeCtx.stop();
        }
    }

    @Override
    public void containsKey(final byte[] key, final KVStoreClosure closure) {
        Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
        ////final Timer.Context timeCtx = getTimeContext("CONTAINS_KEY");
        try {
            final boolean exists = this.defaultDB.containsKey(new PMemDecoratedKey(key));
            setSuccess(closure, exists);
        } catch (final Exception e) {
            LOG.error("Fail to [CONTAINS_KEY], key: [{}], {}.", BytesUtil.readUtf8(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [CONTAINS_KEY]");
        } finally {
            ////timeCtx.stop();
        }
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit,
                     @SuppressWarnings("unused") final boolean readOnlySafe, final boolean returnValue,
                     final KVStoreClosure closure) {
        Requires.requireTrue(startKey == null || startKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(endKey == null || endKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        ////final Timer.Context timeCtx = getTimeContext("SCAN");
        final List<KVEntry> entries = Lists.newArrayList();
        // If limit == 0, it will be modified to Integer.MAX_VALUE on the server
        // and then queried.  So 'limit == 0' means that the number of queries is
        // not limited. This is because serialization uses varint to compress
        // numbers.  In the case of 0, only 1 byte is occupied, and Integer.MAX_VALUE
        // takes 5 bytes.
        final int maxCount = limit > 0 ? limit : Integer.MAX_VALUE;
        final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
        try {
            ConcurrentNavigableMap<PMemDecoratedKey, PersistentImmutableByteArray> subMap;
            if (startKey == null && endKey == null) {
                subMap = this.defaultDB;
            } else if (startKey == null) {
                subMap = this.defaultDB.headMap(new PMemDecoratedKey(endKey), false);
            } else if (endKey == null) {
                subMap = this.defaultDB.tailMap(new PMemDecoratedKey(startKey));
            } else {
                subMap = this.defaultDB.subMap(new PMemDecoratedKey(startKey), new PMemDecoratedKey(endKey));
            }
            if (!subMap.isEmpty()) {
                for (Map.Entry<PMemDecoratedKey, PersistentImmutableByteArray> e : subMap.entrySet()) {
                    if (entries.size() > maxCount) {
                        break;
                    }
                    entries.add(new KVEntry(e.getKey().getKey(), returnValue ? e.getValue().toArray() : null));
                }
            }
            setSuccess(closure, entries);
        } catch (final Exception e) {
            LOG.error("Fail to [SCAN], range: ['[{}, {})'], {}.", BytesUtil.readUtf8(realStartKey),
                BytesUtil.readUtf8(endKey), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [SCAN]");
        } finally {
            ////timeCtx.stop();
        }
    }

    @Override
    public void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure) {
        Requires.requireTrue(seqKey == null || seqKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        ////final Timer.Context timeCtx = getTimeContext("GET_SEQUENCE");
        final byte[] realKey = BytesUtil.nullToEmpty(seqKey);
        try {
            final PersistentLong startLong = this.sequenceDB.get(new PMemDecoratedKey(realKey));
            long startVal = (startLong == null) ? 0 : startLong.longValue();
            if (step < 0) {
                // never get here
                setFailure(closure, "Fail to [GET_SEQUENCE], step must >= 0");
                return;
            }
            if (step == 0) {
                setSuccess(closure, new Sequence(startVal, startVal));
                return;
            }
            final long endVal = getSafeEndValueForSequence(startVal, step);
            if (startVal != endVal) {
                this.sequenceDB.put(new PMemDecoratedKey(realKey), new PersistentLong(endVal));
            }
            setSuccess(closure, new Sequence(startVal, endVal));
        } catch (final Exception e) {
            LOG.error("Fail to [GET_SEQUENCE], [key = {}, step = {}], {}.", BytesUtil.readUtf8(realKey), step,
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [GET_SEQUENCE]", e);
        } finally {
            ////timeCtx.stop();
        }
    }

    @Override
    public void resetSequence(final byte[] seqKey, final KVStoreClosure closure) {
        Requires.requireTrue(seqKey != null && seqKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        ////final Timer.Context timeCtx = getTimeContext("RESET_SEQUENCE");
        try {
            this.sequenceDB.remove(new PMemDecoratedKey(seqKey));
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [RESET_SEQUENCE], [key = {}], {}.", BytesUtil.readUtf8(seqKey),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [RESET_SEQUENCE]", e);
        } finally {
            ////timeCtx.stop();
        }
    }

    @Override
    public void put(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("PUT");
        try {
            this.defaultDB.put(new PMemDecoratedKey(key), new PersistentImmutableByteArray(value));
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [PUT], [{}, {}], kvsize [{}, {}], {}.", BytesUtil.readUtf8(key),
                BytesUtil.readUtf8(value), key.length, value == null ? 0 : value.length, StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(value.length <= PMemDBOptions.MAX_VALUE_SIZE);
        //final Timer.Context timeCtx = getTimeContext("GET_PUT");
        try {
            final PMemDecoratedKey k = new PMemDecoratedKey(key);
            final PersistentImmutableByteArray updateVal = new PersistentImmutableByteArray(value);
            PersistentImmutableByteArray prevVal = null;
            do {
                if (this.defaultDB.containsKey(k)) {
                    prevVal = this.defaultDB.get(k);
                    if (prevVal == null) {
                        if (this.defaultDB.replace(k, updateVal) == null) {
                            break;
                        }
                    } else if (this.defaultDB.replace(k, prevVal, updateVal)) {
                        break;
                    }
                } else {
                    if ((prevVal = this.defaultDB.putIfAbsent(k, updateVal)) == null) {
                        break;
                    }
                }
            } while (true);
            setSuccess(closure, prevVal == null ? null : prevVal.toArray());
        } catch (final Exception e) {
            LOG.error("Fail to [GET_PUT], [{}, {}], {}.", BytesUtil.readUtf8(key), BytesUtil.readUtf8(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [GET_PUT]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(expect.length <= PMemDBOptions.MAX_VALUE_SIZE);
        Requires.requireTrue(update.length <= PMemDBOptions.MAX_VALUE_SIZE);
        //final Timer.Context timeCtx = getTimeContext("COMPARE_PUT");
        try {
            final PMemDecoratedKey k = new PMemDecoratedKey(key);
            final PersistentImmutableByteArray expectVal = new PersistentImmutableByteArray(expect);
            final PersistentImmutableByteArray updateVal = new PersistentImmutableByteArray(update);
            boolean success = this.defaultDB.replace(k, expectVal, updateVal);
            setSuccess(closure, success);
        } catch (final Exception e) {
            LOG.error("Fail to [COMPARE_PUT], [{}, {}, {}], {}.", BytesUtil.readUtf8(key), BytesUtil.readUtf8(expect),
                BytesUtil.readUtf8(update), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [COMPARE_PUT]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void merge(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(value.length <= PMemDBOptions.MAX_VALUE_SIZE);
        //final Timer.Context timeCtx = getTimeContext("MERGE");
        try {
            final PMemDecoratedKey k = new PMemDecoratedKey(key);
            PersistentImmutableByteArray expectVal;
            PersistentImmutableByteArray updateVal;
            do {
                expectVal = this.defaultDB.get(k);
                byte[] oldValue = expectVal.toArray();
                byte[] newValue = null;
                if (expectVal == null || expectVal.length() == 0) {
                    updateVal = new PersistentImmutableByteArray(value);
                } else {
                    newValue = new byte[oldValue.length + 1 + value.length];
                    System.arraycopy(oldValue, 0, newValue, 0, oldValue.length);
                    newValue[oldValue.length] = DELIMITER;
                    System.arraycopy(value, 0, newValue, oldValue.length + 1, value.length);
                    updateVal = new PersistentImmutableByteArray(newValue);
                }
                if (Arrays.equals(oldValue, newValue)) {
                    break;
                }
            } while (!this.defaultDB.replace(k, expectVal, updateVal));
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [MERGE], [{}, {}], {}.", BytesUtil.readUtf8(key), BytesUtil.readUtf8(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [MERGE]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void put(final List<KVEntry> entries, final KVStoreClosure closure) {
        //final Timer.Context timeCtx = getTimeContext("PUT_LIST");
        try {
            for (final KVEntry entry : entries) {
                final byte[] key = entry.getKey();
                final byte[] value = entry.getValue();
                Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
                Requires.requireTrue(value.length <= PMemDBOptions.MAX_VALUE_SIZE);
                this.defaultDB.put(new PMemDecoratedKey(key), new PersistentImmutableByteArray(value));
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [PUT_LIST], [size = {}], {}.", entries.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT_LIST]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(value.length <= PMemDBOptions.MAX_VALUE_SIZE);
        //final Timer.Context timeCtx = getTimeContext("PUT_IF_ABSENT");
        try {
            final PMemDecoratedKey k = new PMemDecoratedKey(key);
            final PersistentImmutableByteArray prevVal = this.defaultDB.putIfAbsent(k,
                new PersistentImmutableByteArray(value));
            setSuccess(closure, prevVal == null ? null : prevVal.toArray());
        } catch (final Exception e) {
            LOG.error("Fail to [PUT_IF_ABSENT], [{}, {}], {}.", BytesUtil.readUtf8(key), BytesUtil.readUtf8(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT_IF_ABSENT]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                            final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("TRY_LOCK");
        try {
            // The algorithm relies on the assumption that while there is no
            // synchronized clock across the processes, still the local time in
            // every process flows approximately at the same rate, with an error
            // which is small compared to the auto-release time of the lock.
            final long now = acquirer.getLockingTimestamp();
            final long timeoutMillis = acquirer.getLeaseMillis();
            final PMemDecoratedKey k = new PMemDecoratedKey(key);
            final PersistentImmutableByteArray prevV = this.lockerDB.get(k);
            final byte[] prevBytesVal = prevV == null ? null : prevV.toArray();

            final DistributedLock.Owner owner;
            // noinspection ConstantConditions
            do {
                final DistributedLock.OwnerBuilder builder = DistributedLock.newOwnerBuilder();
                if (prevBytesVal == null) {
                    // no others own this lock
                    if (keepLease) {
                        // it wants to keep the lease but too late, will return failure
                        owner = builder //
                            // set acquirer id
                            .id(acquirer.getId())
                            // fail to keep lease
                            .remainingMillis(DistributedLock.OwnerBuilder.KEEP_LEASE_FAIL)
                            // set failure
                            .success(false).build();
                        break;
                    }
                    // is first time to try lock (another possibility is that this lock has been deleted),
                    // will return successful
                    owner = builder //
                        // set acquirer id, now it will own the lock
                        .id(acquirer.getId())
                        // set a new deadline
                        .deadlineMillis(now + timeoutMillis)
                        // first time to acquire and success
                        .remainingMillis(DistributedLock.OwnerBuilder.FIRST_TIME_SUCCESS)
                        // create a new fencing token
                        .fencingToken(getNextFencingToken(fencingKey))
                        // init acquires
                        .acquires(1)
                        // set acquirer ctx
                        .context(acquirer.getContext())
                        // set successful
                        .success(true).build();
                    this.lockerDB.put(k, new PersistentImmutableByteArray(this.serializer.writeObject(owner)));
                    break;
                }

                // this lock has an owner, check if it has expired
                final DistributedLock.Owner prevOwner = this.serializer.readObject(prevBytesVal,
                    DistributedLock.Owner.class);
                final long remainingMillis = prevOwner.getDeadlineMillis() - now;
                if (remainingMillis < 0) {
                    // the previous owner is out of lease
                    if (keepLease) {
                        // it wants to keep the lease but too late, will return failure
                        owner = builder //
                            // still previous owner id
                            .id(prevOwner.getId())
                            // do not update
                            .deadlineMillis(prevOwner.getDeadlineMillis())
                            // fail to keep lease
                            .remainingMillis(DistributedLock.OwnerBuilder.KEEP_LEASE_FAIL)
                            // set previous ctx
                            .context(prevOwner.getContext())
                            // set failure
                            .success(false).build();
                        break;
                    }
                    // create new lock owner
                    owner = builder //
                        // set acquirer id, now it will own the lock
                        .id(acquirer.getId())
                        // set a new deadline
                        .deadlineMillis(now + timeoutMillis)
                        // success as a new acquirer
                        .remainingMillis(DistributedLock.OwnerBuilder.NEW_ACQUIRE_SUCCESS)
                        // create a new fencing token
                        .fencingToken(getNextFencingToken(fencingKey))
                        // init acquires
                        .acquires(1)
                        // set acquirer ctx
                        .context(acquirer.getContext())
                        // set successful
                        .success(true).build();
                    this.lockerDB.put(k, new PersistentImmutableByteArray(this.serializer.writeObject(owner)));
                    break;
                }

                // the previous owner is not out of lease (remainingMillis >= 0)
                final boolean isReentrant = prevOwner.isSameAcquirer(acquirer);
                if (isReentrant) {
                    // is the same old friend come back (reentrant lock)
                    if (keepLease) {
                        // the old friend only wants to keep lease of lock
                        owner = builder //
                            // still previous owner id
                            .id(prevOwner.getId())
                            // update the deadline to keep lease
                            .deadlineMillis(now + timeoutMillis)
                            // success to keep lease
                            .remainingMillis(DistributedLock.OwnerBuilder.KEEP_LEASE_SUCCESS)
                            // keep fencing token
                            .fencingToken(prevOwner.getFencingToken())
                            // keep acquires
                            .acquires(prevOwner.getAcquires())
                            // do not update ctx when keeping lease
                            .context(prevOwner.getContext())
                            // set successful
                            .success(true).build();
                        this.defaultDB.put(k, new PersistentImmutableByteArray(this.serializer.writeObject(owner)));
                        break;
                    }
                    // now we are sure that is an old friend who is back again (reentrant lock)
                    owner = builder //
                        // still previous owner id
                        .id(prevOwner.getId())
                        // by the way, the lease will also be kept
                        .deadlineMillis(now + timeoutMillis)
                        // success reentrant
                        .remainingMillis(DistributedLock.OwnerBuilder.REENTRANT_SUCCESS)
                        // keep fencing token
                        .fencingToken(prevOwner.getFencingToken())
                        // acquires++
                        .acquires(prevOwner.getAcquires() + 1)
                        // update ctx when reentrant
                        .context(acquirer.getContext())
                        // set successful
                        .success(true).build();
                    this.lockerDB.put(k, new PersistentImmutableByteArray(this.serializer.writeObject(owner)));
                    break;
                }

                // the lock is exist and also prev locker is not the same as current
                owner = builder //
                    // set previous owner id to tell who is the real owner
                    .id(prevOwner.getId())
                    // set the remaining lease time of current owner
                    .remainingMillis(remainingMillis)
                    // set previous ctx
                    .context(prevOwner.getContext())
                    // set failure
                    .success(false).build();
                LOG.debug("Another locker [{}] is trying the existed lock [{}].", acquirer, prevOwner);
            } while (false);

            setSuccess(closure, owner);
        } catch (final Exception e) {
            LOG.error("Fail to [TRY_LOCK], [{}, {}], {}.", BytesUtil.readUtf8(key), acquirer,
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [TRY_LOCK]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("RELEASE_LOCK");
        try {
            final PMemDecoratedKey k = new PMemDecoratedKey(key);
            final byte[] prevBytesVal = this.lockerDB.get(k).toArray();

            final DistributedLock.Owner owner;
            // noinspection ConstantConditions
            do {
                final DistributedLock.OwnerBuilder builder = DistributedLock.newOwnerBuilder();
                if (prevBytesVal == null) {
                    LOG.warn("Lock not exist: {}.", acquirer);
                    owner = builder //
                        // set acquirer id
                        .id(acquirer.getId())
                        // set acquirer fencing token
                        .fencingToken(acquirer.getFencingToken())
                        // set acquires=0
                        .acquires(0)
                        // set successful
                        .success(true).build();
                    break;
                }

                final DistributedLock.Owner prevOwner = this.serializer.readObject(prevBytesVal,
                    DistributedLock.Owner.class);

                if (prevOwner.isSameAcquirer(acquirer)) {
                    final long acquires = prevOwner.getAcquires() - 1;
                    owner = builder //
                        // still previous owner id
                        .id(prevOwner.getId())
                        // do not update deadline
                        .deadlineMillis(prevOwner.getDeadlineMillis())
                        // keep fencing token
                        .fencingToken(prevOwner.getFencingToken())
                        // acquires--
                        .acquires(acquires)
                        // set previous ctx
                        .context(prevOwner.getContext())
                        // set successful
                        .success(true).build();
                    if (acquires <= 0) {
                        // real delete, goodbye ~
                        this.lockerDB.remove(k);
                    } else {
                        // acquires--
                        this.lockerDB.put(k, new PersistentImmutableByteArray(this.serializer.writeObject(owner)));
                    }
                    break;
                }

                // invalid acquirer, can't to release the lock
                owner = builder //
                    // set previous owner id to tell who is the real owner
                    .id(prevOwner.getId())
                    // keep previous fencing token
                    .fencingToken(prevOwner.getFencingToken())
                    // do not update acquires
                    .acquires(prevOwner.getAcquires())
                    // set previous ctx
                    .context(prevOwner.getContext())
                    // set failure
                    .success(false).build();
                LOG.warn("The lock owner is: [{}], [{}] could't release it.", prevOwner, acquirer);
            } while (false);

            setSuccess(closure, owner);
        } catch (final Exception e) {
            LOG.error("Fail to [RELEASE_LOCK], [{}], {}.", BytesUtil.readUtf8(key), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [RELEASE_LOCK]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    private long getNextFencingToken(final byte[] fencingKey) {
        Requires.requireTrue(fencingKey == null || fencingKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("FENCING_TOKEN");
        try {
            final byte[] realKey = BytesUtil.nullToEmpty(fencingKey);
            final PMemDecoratedKey k = new PMemDecoratedKey(realKey);
            final PersistentLong v = this.fencingKeyDB.get(k);
            final long prevVal;
            if (v == null) {
                prevVal = 0; // init
            } else {
                prevVal = v.longValue();
            }
            // Don't worry about the token number overflow.
            // It takes about 290,000 years for the 1 million TPS system
            // to use the numbers in the range [0 ~ Long.MAX_VALUE].
            final long newVal = prevVal + 1;
            this.fencingKeyDB.put(k, new PersistentLong(newVal));
            return newVal;
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void delete(final byte[] key, final KVStoreClosure closure) {
        Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("DELETE");
        try {
            this.defaultDB.remove(new PMemDecoratedKey(key));
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [DELETE], [{}], {}.", BytesUtil.readUtf8(key), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        Requires.requireTrue(startKey == null || startKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(endKey == null || endKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("DELETE_RANGE");

        // TODO : thread safe to delete range?
        // TODO : Use iterator to delete range
        try {
            List<PMemDecoratedKey> toDeleteKeys = new LinkedList<>(this.defaultDB.subMap(
                startKey == null ? null : new PMemDecoratedKey(startKey),
                endKey == null ? null : new PMemDecoratedKey(endKey)).keySet());
            for (PMemDecoratedKey k : toDeleteKeys) {
                this.defaultDB.remove(k);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [DELETE_RANGE], ['[{}, {})'], {}.", BytesUtil.readUtf8(startKey),
                BytesUtil.readUtf8(endKey), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE_RANGE]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void delete(final List<byte[]> keys, final KVStoreClosure closure) {
        //final Timer.Context timeCtx = getTimeContext("DELETE_LIST");
        try {
            for (final byte[] key : keys) {
                Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
                this.defaultDB.remove(new PMemDecoratedKey(key));
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [DELETE_LIST], [size = {}], {}.", keys.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE_LIST]", e);
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void batch(final List<KVOperation> kvOperations, final KVStoreClosure closure) {
        //final Timer.Context timeCtx = getTimeContext("BATCH_OP");
        try {
            doBatch(kvOperations, closure);
        } catch (final Exception e) {
            LOG.error("Failed to [BATCH_OP], [size = {}], {}.", kvOperations.size(), StackTraceUtil.stackTrace(e));
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void destroy(final long regionId, final KVStoreClosure closure) {
        throw new UnsupportedOperationException("destroy is not implemented yet");
    }

    @Override
    public void seal(final long regionId, final KVStoreClosure closure) {
        throw new UnsupportedOperationException("seal is not implemented yet");
    }

    @Override
    public void isSealed(final long regionId, final KVStoreClosure closure) {
        throw new UnsupportedOperationException("isSealed is not implemented yet");
    }

    @Override
    public long getApproximateKeysInRange(final byte[] startKey, final byte[] endKey) {
        Requires.requireTrue(startKey != null || endKey != null);
        Requires.requireTrue(startKey == null || startKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(endKey == null || endKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("APPROXIMATE_KEYS");
        try {
            ConcurrentNavigableMap<PMemDecoratedKey, PersistentImmutableByteArray> subMap;
            if (startKey == null && endKey == null) {
                subMap = this.defaultDB;
            } else if (startKey == null) {
                subMap = this.defaultDB.headMap(new PMemDecoratedKey(endKey), false);
            } else if (endKey == null) {
                subMap = this.defaultDB.tailMap(new PMemDecoratedKey(startKey));
            } else {
                subMap = this.defaultDB.subMap(new PMemDecoratedKey(startKey), new PMemDecoratedKey(endKey));
            }
            return subMap.size();
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public byte[] jumpOver(final byte[] startKey, final long distance) {
        Requires.requireTrue(startKey == null || startKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("JUMP_OVER");
        try {
            KVIterator it = new PMemKVIterator(this.defaultDB);
            final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
            it.seek(realStartKey);
            if (!it.isValid()) {
                return null;
            }

            long approximateKeys = 0;
            byte[] lastKey = null;
            while (it.isValid()) {
                lastKey = it.key();
                if (++approximateKeys > distance) {
                    break;
                }

                it.next();
            }
            if (lastKey == null) {
                return null;
            }
            final byte[] endKey = new byte[lastKey.length];
            System.arraycopy(lastKey, 0, endKey, 0, lastKey.length);
            return endKey;
        } finally {
            //timeCtx.stop();
        }
    }

    @Override
    public void initFencingToken(final byte[] parentKey, final byte[] childKey) {
        Requires.requireTrue(parentKey == null || parentKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(childKey != null && childKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        //final Timer.Context timeCtx = getTimeContext("INIT_FENCING_TOKEN");
        try {
            // TODO : make 'CAS' atomic
            final byte[] realKey = BytesUtil.nullToEmpty(parentKey);
            final PMemDecoratedKey k = new PMemDecoratedKey(realKey);
            final PMemDecoratedKey ck = new PMemDecoratedKey(childKey);
            final PersistentLong parentVal = this.fencingKeyDB.get(k);
            if (parentVal == null) {
                return;
            }
            this.fencingKeyDB.put(ck, parentVal);
        } finally {
            //timeCtx.stop();
        }
    }

    void doSnapshotSave(final PMemKVStoreSnapshotFile snapshotFile, final String snapshotPath, final Region region)
                                                                                                                   throws Exception {
        //final Timer.Context timeCtx = getTimeContext("SNAPSHOT_SAVE");
        // TODO : do transactional save for all DBs (defaultDB and sequenceDB, fencingDB ...)
        snapshotLock.writeLock().lock();
        try {
            final String tempPath = snapshotPath + "_temp";
            final File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            FileUtils.forceMkdir(tempFile);

            snapshotFile.writeToFile(tempPath, "sequenceDB",
                new PMemKVStoreSnapshotFile.SequenceDB(subRangeMap(this.sequenceDB, region)));
            snapshotFile.writeToFile(tempPath, "fencingKeyDB",
                new PMemKVStoreSnapshotFile.FencingKeyDB(subRangeMap(this.fencingKeyDB, region)));
            snapshotFile.writeToFile(tempPath, "lockerDB",
                new PMemKVStoreSnapshotFile.LockerDB(subRangeMapOwner(this.lockerDB, region)));
            final int size = this.opts.getKeysPerSegment();
            final List<Pair<byte[], byte[]>> segment = Lists.newArrayListWithCapacity(size);
            int index = 0;
            final byte[] realStartKey = BytesUtil.nullToEmpty(region.getStartKey());
            final byte[] endKey = region.getEndKey();

            KVIterator it = new PMemKVIterator(this.defaultDB);
            it.seek(realStartKey);
            while (it.isValid()) {
                if (endKey != null && BytesUtil.compare(it.key(), endKey) >= 0) {
                    break;
                }

                segment.add(Pair.of(it.key(), it.value()));
                if (segment.size() >= size) {
                    snapshotFile.writeToFile(tempPath, "segment" + index++,
                        new PMemKVStoreSnapshotFile.Segment(segment));
                    segment.clear();
                }
                it.next();
            }

            if (!segment.isEmpty()) {
                snapshotFile.writeToFile(tempPath, "segment" + index++, new PMemKVStoreSnapshotFile.Segment(segment));
            }
            snapshotFile.writeToFile(tempPath, "tailIndex", new PMemKVStoreSnapshotFile.TailIndex(--index));

            final File destinationPath = new File(snapshotPath);
            FileUtils.deleteDirectory(destinationPath);
            FileUtils.moveDirectory(tempFile, destinationPath);
        } finally {
            snapshotLock.writeLock().unlock();
            //timeCtx.stop();
        }
    }

    void doSnapshotLoad(final PMemKVStoreSnapshotFile snapshotFile, final String snapshotPath) throws Exception {
        //final Timer.Context timeCtx = getTimeContext("SNAPSHOT_LOAD");
        snapshotLock.readLock().lock();
        try {
            final PMemKVStoreSnapshotFile.SequenceDB sequenceDB = snapshotFile.readFromFile(snapshotPath, "sequenceDB",
                PMemKVStoreSnapshotFile.SequenceDB.class);
            final PMemKVStoreSnapshotFile.FencingKeyDB fencingKeyDB = snapshotFile.readFromFile(snapshotPath,
                "fencingKeyDB", PMemKVStoreSnapshotFile.FencingKeyDB.class);
            final PMemKVStoreSnapshotFile.LockerDB lockerDB = snapshotFile.readFromFile(snapshotPath, "lockerDB",
                PMemKVStoreSnapshotFile.LockerDB.class);

            this.dump(this.sequenceDB, sequenceDB.data());
            this.dump(this.fencingKeyDB, fencingKeyDB.data());
            this.dump(this.lockerDB, lockerDB.data());

            final PMemKVStoreSnapshotFile.TailIndex tailIndex = snapshotFile.readFromFile(snapshotPath, "tailIndex",
                PMemKVStoreSnapshotFile.TailIndex.class);
            final int tail = tailIndex.data();
            final List<PMemKVStoreSnapshotFile.Segment> segments = Lists.newArrayListWithCapacity(tail + 1);
            for (int i = 0; i <= tail; i++) {
                final PMemKVStoreSnapshotFile.Segment segment = snapshotFile.readFromFile(snapshotPath, "segment" + i,
                    PMemKVStoreSnapshotFile.Segment.class);
                segments.add(segment);
            }
            for (final PMemKVStoreSnapshotFile.Segment segment : segments) {
                for (final Pair<byte[], byte[]> p : segment.data()) {
                    this.defaultDB
                        .put(new PMemDecoratedKey(p.getKey()), new PersistentImmutableByteArray(p.getValue()));
                }
            }
        } finally {
            snapshotLock.readLock().unlock();
            //timeCtx.stop();
        }
    }

    private <P extends AnyPersistent, T> void dump(PersistentSIHashMap<PMemDecoratedKey, P> db, Map<ByteArray, T> data) {
        for (Map.Entry<ByteArray, T> e : data.entrySet()) {
            final byte[] key = e.getKey().getBytes();
            final PMemDecoratedKey k = new PMemDecoratedKey(key);
            T value = e.getValue();
            if (value instanceof Long) {
                ((PersistentSIHashMap<PMemDecoratedKey, PersistentLong>) db).put(k, new PersistentLong((Long) value));
            } else if (value instanceof DistributedLock.Owner) {
                ((PersistentSIHashMap<PMemDecoratedKey, PersistentImmutableByteArray>) db).put(k,
                    new PersistentImmutableByteArray(this.serializer.writeObject(value)));
            } else {
                throw new RuntimeException("dump unsupported value type " + value.getClass().getSimpleName());
            }
        }
    }

    private Map<ByteArray, DistributedLock.Owner> subRangeMapOwner(final PersistentSIHashMap<PMemDecoratedKey, PersistentImmutableByteArray> input,
                                                                   final Region region) {
        final Map<ByteArray, DistributedLock.Owner> output = new HashMap<>();
        if (RegionHelper.isSingleGroup(region)) {
            for (Map.Entry<PMemDecoratedKey, PersistentImmutableByteArray> e : input.entrySet()) {
                output.put(ByteArray.wrap(e.getKey().toArray()),
                    this.serializer.readObject(e.getValue().toArray(), DistributedLock.Owner.class));
            }
            return output;
        }

        final Iterator<Map.Entry<PMemDecoratedKey, PersistentImmutableByteArray>> it = input.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<PMemDecoratedKey, PersistentImmutableByteArray> e = it.next();
            byte[] key = e.getKey().toArray();
            byte[] value = e.getValue().toArray();
            if (RegionHelper.isKeyInRegion(key, region)) {
                output.put(ByteArray.wrap(key), this.serializer.readObject(value, DistributedLock.Owner.class));
            }
        }
        return output;
    }
}
