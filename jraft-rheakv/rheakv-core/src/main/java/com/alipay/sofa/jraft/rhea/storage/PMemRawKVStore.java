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

import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.*;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.codahale.metrics.Timer;
import io.pmem.pmemkv.Database;
import io.pmem.pmemkv.GetAllByteArrayCallback;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Jerry Yang
 * TODO :
 * 1. support customized comparator
 * 2. improve Iterator, no key cache.
 * 3. use more persistent data structure
 */
public class PMemRawKVStore extends BatchRawKVStore<PMemDBOptions> {

    private static final Logger             LOG           = LoggerFactory.getLogger(PMemRawKVStore.class);

    private final ReadWriteLock             readWriteLock = new ReentrantReadWriteLock();
    private final Serializer                serializer    = Serializers.getDefault();

    private static final byte               DELIMITER     = (byte) ',';
    private static final Comparator<byte[]> COMPARATOR    = BytesUtil.getDefaultByteArrayComparator();

    private final long                      regionId;
    private boolean                         writable      = true;

    private static final String[]           DB_FILE_NAMES = new String[] { "defaultDB", "sequenceDB", "fencingKeyDB",
            "lockerDB"                                   };
    private Database                        defaultDB;
    private Database                        sequenceDB;
    private Database                        fencingKeyDB;
    private Database                        lockerDB;
    private volatile PMemDBOptions          opts;

    public PMemRawKVStore() {
        this(-1L);
    }

    public PMemRawKVStore(final long regionId) {
        super();
        this.regionId = regionId;
    }

    @Override
    public boolean init(final PMemDBOptions opts) {
        this.opts = opts;

        Requires.requireTrue(hasEnoughSpace(opts), "No enough space for Persistent Memory on "
                                                   + PMemDBOptions.PMEM_ROOT_PATH);

        if (opts.getPmemDataSize() > 0) {
            this.defaultDB = new Database(opts.getOrderedEngine(), generateConf(opts.getOrderedEngine(),
                opts.getDbPath(), DB_FILE_NAMES[0], opts.getPmemDataSize(), opts.getForceCreate() ? 1 : 0));
        }
        if (opts.getPmemMetaSize() > 0) {
            this.sequenceDB = new Database(opts.getHashEngine(), generateConf(opts.getHashEngine(), opts.getDbPath(),
                DB_FILE_NAMES[1], opts.getPmemMetaSize(), opts.getForceCreate() ? 1 : 0));
            this.fencingKeyDB = new Database(opts.getHashEngine(), generateConf(opts.getHashEngine(), opts.getDbPath(),
                DB_FILE_NAMES[2], opts.getPmemMetaSize(), opts.getForceCreate() ? 1 : 0));
            this.lockerDB = new Database(opts.getHashEngine(), generateConf(opts.getHashEngine(), opts.getDbPath(),
                DB_FILE_NAMES[3], opts.getPmemMetaSize(), opts.getForceCreate() ? 1 : 0));
        }
        LOG.info("[PMemRawKVStore] start successfully, options: {}.", opts);
        return true;
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
                            Files.createDirectories(Paths.get(parentPath));
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

    @Override
    public void shutdown() {
        if (this.defaultDB != null) {
            this.defaultDB.stop();
            this.defaultDB = null;
        }
        if (this.sequenceDB != null) {
            this.sequenceDB.stop();
            this.sequenceDB = null;
        }
        if (this.fencingKeyDB != null) {
            this.fencingKeyDB.stop();
            this.fencingKeyDB = null;
        }
        if (this.lockerDB != null) {
            this.lockerDB.stop();
            this.lockerDB = null;
        }
    }

    @Override
    public KVIterator localIterator() {
        return new PMemKVIterator2(this.defaultDB);
    }

    @Override
    public void get(final byte[] key, @SuppressWarnings("unused") final boolean readOnlySafe,
                    final KVStoreClosure closure) {
        Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("GET");
        readLock().lock();
        try {
            final byte[] value = this.defaultDB.get(key);
            setSuccess(closure, value);
        } catch (final Exception e) {
            LOG.error("Fail to [GET], key: [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [GET]");
        } finally {
            readLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void multiGet(final List<byte[]> keys, @SuppressWarnings("unused") final boolean readOnlySafe,
                         final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("MULTI_GET");
        readLock().lock();
        try {
            final Map<ByteArray, byte[]> resultMap = Maps.newHashMap();
            for (final byte[] key : keys) {
                Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
                final byte[] value = this.defaultDB.get(key);
                resultMap.put(ByteArray.wrap(key), value);
            }
            setSuccess(closure, resultMap);
        } catch (final Exception e) {
            LOG.error("Fail to [MULTI_GET], key size: [{}], {}.", keys.size(), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [MULTI_GET]");
        } finally {
            readLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void containsKey(final byte[] key, final KVStoreClosure closure) {
        Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("CONTAINS_KEY");
        try {
            final boolean exists = this.defaultDB.exists(key);
            setSuccess(closure, exists);
        } catch (final Exception e) {
            LOG.error("Fail to [CONTAINS_KEY], key: [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [CONTAINS_KEY]");
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit,
                     @SuppressWarnings("unused") final boolean readOnlySafe, final boolean returnValue,
                     final KVStoreClosure closure) {
        Requires.requireTrue(startKey == null || startKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(endKey == null || endKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("SCAN");
        final List<KVEntry> entries = Lists.newArrayList();
        // If limit == 0, it will be modified to Integer.MAX_VALUE on the server
        // and then queried.  So 'limit == 0' means that the number of queries is
        // not limited. This is because serialization uses varint to compress
        // numbers.  In the case of 0, only 1 byte is occupied, and Integer.MAX_VALUE
        // takes 5 bytes.
        final int maxCount = limit > 0 ? limit : Integer.MAX_VALUE;
        final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);

        final GetAllByteArrayCallback getKVCallback = (byte[] k, byte[] v) -> {
            if (entries.size() < maxCount) {
                entries.add(new KVEntry(k, returnValue ? v : null));
            }
        };
        readLock().lock();
        // TODO : stree doesn't support get_between API yet.
        try {
            if (endKey == null) {
                this.defaultDB.get_above(realStartKey, getKVCallback);
            } else {
                this.defaultDB.get_between(realStartKey, endKey, getKVCallback);
            }
            setSuccess(closure, entries);
        } catch (final Exception e) {
            LOG.error("Fail to [SCAN], range: ['[{}, {})'], {}.", BytesUtil.toHex(realStartKey), BytesUtil.toHex(endKey),
                    StackTraceUtil.stackTrace(e));
            setFailure(closure, "Fail to [SCAN]");
        } finally {
            readLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure) {
        Requires.requireTrue(seqKey == null || seqKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("GET_SEQUENCE");
        writeLock().lock();
        final byte[] realKey = BytesUtil.nullToEmpty(seqKey);
        try {
            byte[] startBytes = this.sequenceDB.get(realKey);
            long startVal = (startBytes == null || startBytes.length == 0) ? 0 : ByteArray.getLong(startBytes);
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
                // cmap is thread safe, no lock is needed
                this.sequenceDB.put(realKey, ByteArray.convertToBytes(endVal));
            }
            setSuccess(closure, new Sequence(startVal, endVal));
        } catch (final Exception e) {
            LOG.error("Fail to [GET_SEQUENCE], [key = {}, step = {}], {}.", BytesUtil.toHex(realKey), step,
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [GET_SEQUENCE]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void resetSequence(final byte[] seqKey, final KVStoreClosure closure) {
        Requires.requireTrue(seqKey == null || seqKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("RESET_SEQUENCE");
        try {
            checkWritable();
            this.sequenceDB.remove(seqKey);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [RESET_SEQUENCE], [key = {}], {}.", BytesUtil.toHex(seqKey),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [RESET_SEQUENCE]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void put(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("PUT");
        writeLock().lock();
        try {
            checkWritable();
            this.defaultDB.put(key, value);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [PUT], [{}, {}], kvsize [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                key.length, value == null ? 0 : value.length, StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(value.length <= PMemDBOptions.MAX_VALUE_SIZE);
        final Timer.Context timeCtx = getTimeContext("GET_PUT");
        // TODO : check if the pmemkv doesn't support concurrent write
        writeLock().lock();
        try {
            checkWritable();
            final byte[] prevVal = this.defaultDB.get(key);
            this.defaultDB.put(key, value);
            setSuccess(closure, prevVal);
        } catch (final Exception e) {
            LOG.error("Fail to [GET_PUT], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [GET_PUT]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(expect.length <= PMemDBOptions.MAX_VALUE_SIZE);
        Requires.requireTrue(update.length <= PMemDBOptions.MAX_VALUE_SIZE);
        final Timer.Context timeCtx = getTimeContext("COMPARE_PUT");
        // TODO : check if the pmemkv doesn't support concurrent write
        writeLock().lock();
        try {
            checkWritable();
            final byte[] actual = this.defaultDB.get(key);
            if (Arrays.equals(expect, actual)) {
                this.defaultDB.put(key, update);
                setSuccess(closure, Boolean.TRUE);
            } else {
                setSuccess(closure, Boolean.FALSE);
            }
        } catch (final Exception e) {
            LOG.error("Fail to [COMPARE_PUT], [{}, {}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(expect),
                BytesUtil.toHex(update), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [COMPARE_PUT]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void merge(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(value.length <= PMemDBOptions.MAX_VALUE_SIZE);
        final Timer.Context timeCtx = getTimeContext("MERGE");
        writeLock().lock();
        try {
            checkWritable();
            byte[] newVal = null;
            byte[] oldVal = this.defaultDB.get(key);
            if (oldVal == null) {
                newVal = value;
            } else {
                newVal = new byte[oldVal.length + 1 + value.length];
                System.arraycopy(oldVal, 0, newVal, 0, oldVal.length);
                newVal[oldVal.length] = DELIMITER;
                System.arraycopy(value, 0, newVal, oldVal.length + 1, value.length);
            }
            if (!Arrays.equals(oldVal, newVal)) {
                this.defaultDB.put(key, newVal);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [MERGE], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [MERGE]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void put(final List<KVEntry> entries, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("PUT_LIST");
        writeLock().lock();
        try {
            checkWritable();
            for (final KVEntry entry : entries) {
                final byte[] key = entry.getKey();
                final byte[] value = entry.getValue();
                Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
                Requires.requireTrue(value.length <= PMemDBOptions.MAX_VALUE_SIZE);
                this.defaultDB.put(key, value);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [PUT_LIST], [size = {}], {}.", entries.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT_LIST]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(value.length <= PMemDBOptions.MAX_VALUE_SIZE);
        final Timer.Context timeCtx = getTimeContext("PUT_IF_ABSENT");
        writeLock().lock();
        try {
            checkWritable();
            final byte[] prevVal = this.defaultDB.get(key);
            Requires.requireTrue(prevVal == null || prevVal.length <= PMemDBOptions.MAX_VALUE_SIZE);
            if (prevVal == null) {
                this.defaultDB.put(key, value);
            }
            setSuccess(closure, prevVal);
        } catch (final Exception e) {
            LOG.error("Fail to [PUT_IF_ABSENT], [{}, {}], {}.", BytesUtil.toHex(key), BytesUtil.toHex(value),
                StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [PUT_IF_ABSENT]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                            final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("TRY_LOCK");
        try {
            checkWritable();
            // The algorithm relies on the assumption that while there is no
            // synchronized clock across the processes, still the local time in
            // every process flows approximately at the same rate, with an error
            // which is small compared to the auto-release time of the lock.
            final long now = acquirer.getLockingTimestamp();
            final long timeoutMillis = acquirer.getLeaseMillis();
            final byte[] prevBytesVal = this.lockerDB.get(key);

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
                    this.lockerDB.put(key, this.serializer.writeObject(owner));
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
                    this.lockerDB.put(key, this.serializer.writeObject(owner));
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
                        this.defaultDB.put(key, this.serializer.writeObject(owner));
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
                    this.lockerDB.put(key, this.serializer.writeObject(owner));
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
            LOG.error("Fail to [TRY_LOCK], [{}, {}], {}.", BytesUtil.toHex(key), acquirer, StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [TRY_LOCK]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        Requires.requireTrue(key.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("RELEASE_LOCK");
        try {
            checkWritable();
            final byte[] prevBytesVal = this.lockerDB.get(key);

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
                        this.lockerDB.remove(key);
                    } else {
                        // acquires--
                        this.lockerDB.put(key, this.serializer.writeObject(owner));
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
            LOG.error("Fail to [RELEASE_LOCK], [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [RELEASE_LOCK]", e);
        } finally {
            timeCtx.stop();
        }
    }

    private long getNextFencingToken(final byte[] fencingKey) {
        Requires.requireTrue(fencingKey == null || fencingKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("FENCING_TOKEN");
        try {
            final byte[] realKey = BytesUtil.nullToEmpty(fencingKey);
            final byte[] prevBytesVal = this.fencingKeyDB.get(realKey);
            final long prevVal;
            if (prevBytesVal == null) {
                prevVal = 0; // init
            } else {
                prevVal = ByteArray.getLong(prevBytesVal);
            }
            // Don't worry about the token number overflow.
            // It takes about 290,000 years for the 1 million TPS system
            // to use the numbers in the range [0 ~ Long.MAX_VALUE].
            final long newVal = prevVal + 1;
            this.fencingKeyDB.put(realKey, ByteArray.convertToBytes(newVal));
            return newVal;
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void delete(final byte[] key, final KVStoreClosure closure) {
        Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("DELETE");
        writeLock().lock();
        try {
            checkWritable();
            this.defaultDB.remove(key);
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [DELETE], [{}], {}.", BytesUtil.toHex(key), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        Requires.requireTrue(startKey == null || startKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(endKey == null || endKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("DELETE_RANGE");
        writeLock().lock();
        try {
            checkWritable();
            List<byte[]> toDeleteKeys = new LinkedList<>();
            final GetAllByteArrayCallback getKVCallback = (byte[] k, byte[] v) -> {
                if (k == null || COMPARATOR.compare(k, endKey) >= 0) {
                    return;
                }
                toDeleteKeys.add(k);
            };
            this.defaultDB.get_between(startKey, endKey, getKVCallback);
            for (byte[] k : toDeleteKeys) {
                this.defaultDB.remove(k);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Fail to [DELETE_RANGE], ['[{}, {})'], {}.", BytesUtil.toHex(startKey), BytesUtil.toHex(endKey),
                    StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE_RANGE]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void delete(final List<byte[]> keys, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("DELETE_LIST");
        writeLock().lock();
        try {
            checkWritable();
            for (final byte[] key : keys) {
                Requires.requireTrue(key != null && key.length <= PMemDBOptions.MAX_KEY_SIZE);
                this.defaultDB.remove(key);
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            LOG.error("Failed to [DELETE_LIST], [size = {}], {}.", keys.size(), StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DELETE_LIST]", e);
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void batch(final List<KVOperation> kvOperations, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("BATCH_OP");
        writeLock().lock();
        try {
            if (kvOperations.stream().anyMatch(KVOperation::isWriteOp)) {
                checkWritable();
            }
            doBatch(kvOperations, closure);
        } catch (final Exception e) {
            LOG.error("Failed to [BATCH_OP], [size = {}], {}.", kvOperations.size(), StackTraceUtil.stackTrace(e));
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void destroy(final long regionId, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("DESTROY");
        try {
            LOG.warn("Start to destroy PMemRawKVStore [regionId = {}]", regionId);
            // Don't check if regionId is -1L,which indicates it belongs to ANY region.
            if (this.regionId != -1L && regionId != this.regionId) {
                throw new IllegalArgumentException(String.format("unexpected regionid %d vs %d", regionId,
                    this.regionId));
            }
            shutdown();
            for (String fileName : DB_FILE_NAMES) {
                Path fullPath = Paths.get(this.opts.getDbPath(), fileName);
                if (Files.exists(fullPath)) {
                    LOG.warn("deleting {} [regionId = {}]", fullPath, this.regionId);
                    Files.deleteIfExists(fullPath);
                }
            }
            Files.delete(Paths.get(this.opts.getDbPath()));
            setSuccess(closure, Boolean.TRUE);
            LOG.warn("Destroyed PMemRawKVStore [regionId = {}] successfully", this.regionId);
        } catch (final Exception e) {
            LOG.error("Failed to [DESTROY], [regionId = {}], {}.", regionId, StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [DESTROY]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public void seal(final long regionId, final KVStoreClosure closure) {
        final Timer.Context timeCtx = getTimeContext("SEAL");
        try {
            LOG.warn("Start to seal PMemRawKVStore [regionId = {}]", regionId);
            // Don't check if regionId is -1L,which indicates it belongs to ANY region.
            if (this.regionId != -1L && regionId != this.regionId) {
                throw new IllegalArgumentException(String.format("unexpected regionid %d vs %d", regionId,
                    this.regionId));
            }
            this.writable = false;
            setSuccess(closure, Boolean.TRUE);
            LOG.warn("Sealed PMemRawKVStore [regionId = {}] successfully", this.regionId);
        } catch (final Exception e) {
            LOG.error("Failed to [SEAL], [regionId = {}], {}.", regionId, StackTraceUtil.stackTrace(e));
            setCriticalError(closure, "Fail to [SEAL]", e);
        } finally {
            timeCtx.stop();
        }
    }

    @Override
    public long getApproximateKeysInRange(final byte[] startKey, final byte[] endKey) {
        Requires.requireTrue(startKey == null || startKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(endKey == null || endKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("APPROXIMATE_KEYS");
        readLock().lock();
        try {
            final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
            return this.defaultDB.countBetween(realStartKey, endKey);
        } finally {
            readLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public byte[] jumpOver(final byte[] startKey, final long distance) {
        Requires.requireTrue(startKey == null || startKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("JUMP_OVER");
        readLock().lock();
        try {
            KVIterator it = new PMemKVIterator2(this.defaultDB);
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
            readLock().unlock();
            timeCtx.stop();
        }
    }

    @Override
    public void initFencingToken(final byte[] parentKey, final byte[] childKey) {
        Requires.requireTrue(parentKey == null || parentKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        Requires.requireTrue(childKey == null || childKey.length <= PMemDBOptions.MAX_KEY_SIZE);
        final Timer.Context timeCtx = getTimeContext("INIT_FENCING_TOKEN");
        try {
            // TODO : make 'CAS' atomic
            final byte[] realKey = BytesUtil.nullToEmpty(parentKey);
            final byte[] parentVal = this.fencingKeyDB.get(realKey);
            if (parentVal == null) {
                return;
            }
            this.fencingKeyDB.put(childKey, parentVal);
        } finally {
            timeCtx.stop();
        }
    }

    void doSnapshotSave(final PMemKVStoreSnapshotFile snapshotFile, final String snapshotPath, final Region region)
                                                                                                                   throws Exception {
        final Timer.Context timeCtx = getTimeContext("SNAPSHOT_SAVE");
        // TODO : do transactional save for all DBs (defaultDB and sequenceDB, fencingDB ...)
        writeLock().lock();
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

            KVIterator it = new PMemKVIterator2(this.defaultDB);
            it.seek(realStartKey);
            while (it.isValid()) {
                if (endKey != null && COMPARATOR.compare(it.key(), endKey) >= 0) {
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
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    void doSnapshotLoad(final PMemKVStoreSnapshotFile snapshotFile, final String snapshotPath) throws Exception {
        final Timer.Context timeCtx = getTimeContext("SNAPSHOT_LOAD");
        writeLock().lock();
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
                    this.defaultDB.put(p.getKey(), p.getValue());
                }
            }
        } finally {
            writeLock().unlock();
            timeCtx.stop();
        }
    }

    private <T> void dump(Database db, Map<ByteArray, T> data) {
        for (Map.Entry<ByteArray, T> e : data.entrySet()) {
            byte[] key = e.getKey().getBytes();
            T value = e.getValue();
            if (value instanceof Long) {
                db.put(key, ByteArray.convertToBytes((Long) value));
            } else if (value instanceof DistributedLock.Owner) {
                db.put(key, this.serializer.writeObject(value));
            }
        }
    }

    static Map<ByteArray, Long> subRangeMap(final Database input, final Region region) {
        final Map<ByteArray, Long> output = new HashMap<>();
        if (RegionHelper.isSingleGroup(region)) {
            GetAllByteArrayCallback cb = (k, v) -> output.put(ByteArray.wrap(k), ByteArray.getLong(v));
            input.get_all(cb);
            return output;
        }

        final KVIterator it = new PMemKVIterator2(input);
        while (it.isValid()) {
            byte[] key = it.key();
            byte[] value = it.value();
            if (RegionHelper.isKeyInRegion(key, region)) {
                output.put(ByteArray.wrap(key), ByteArray.getLong(value));
            }
            it.next();
        }
        return output;
    }

    private Map<ByteArray, DistributedLock.Owner> subRangeMapOwner(final Database input, final Region region) {
        final Map<ByteArray, DistributedLock.Owner> output = new HashMap<>();
        if (RegionHelper.isSingleGroup(region)) {
            GetAllByteArrayCallback cb = (k, v) -> output.put(ByteArray.wrap(k), this.serializer.readObject(v, DistributedLock.Owner.class));
            input.get_all(cb);
            return output;
        }

        final KVIterator it = new PMemKVIterator2(input);
        while (it.isValid()) {
            byte[] key = it.key();
            byte[] value = it.value();
            if (RegionHelper.isKeyInRegion(key, region)) {
                output.put(ByteArray.wrap(key), this.serializer.readObject(value, DistributedLock.Owner.class));
            }
            it.next();
        }
        return output;
    }

    private Lock readLock() {
        return this.readWriteLock.readLock();
    }

    private Lock writeLock() {
        return this.readWriteLock.writeLock();
    }

    private void checkWritable() {
        if (!writable) {
            throw new RheaRuntimeException("region " + regionId + " is sealed");
        }
    }
}
