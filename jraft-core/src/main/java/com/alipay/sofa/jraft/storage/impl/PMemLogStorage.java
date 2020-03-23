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
package com.alipay.sofa.jraft.storage.impl;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.StorageType;
import lib.util.persistent.ObjectDirectory;
import lib.util.persistent.PersistentSkipListMap2;
import lib.util.persistent.PersistentImmutableByteArray;
import lib.util.persistent.PersistentLong;
import lib.util.persistent.spi.PersistentMemoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alipay.sofa.jraft.entity.EnumOutter.EntryType.ENTRY_TYPE_NO_OP;

/**
 * Log storage in persistent memory by pmemlog
 *
 * @author Jerry Yang (20830326@qq.com)
 *
 * 2020-Jan-2 16:00:00
 */
public class PMemLogStorage implements LogStorage, Describer {
    private static final Logger                                                  LOG                     = LoggerFactory
                                                                                                             .getLogger(PMemLogStorage.class);
    static {
        PersistentMemoryProvider.getDefaultProvider().getHeap().open();
    }

    private static final long                                                    DEFAULT_FIRST_LOG_INDEX = 1L;

    private final String                                                         path;
    private PersistentSkipListMap2<PersistentLong, PersistentImmutableByteArray> db;
    private PersistentSkipListMap2<PersistentLong, PersistentImmutableByteArray> confDb;
    private volatile long                                                        firstLogIndex           = DEFAULT_FIRST_LOG_INDEX;
    private volatile boolean                                                     hasLoadFirstLogIndex    = false;
    private final ReadWriteLock                                                  readWriteLock           = new ReentrantReadWriteLock();
    private final Lock                                                           writeLock               = this.readWriteLock
                                                                                                             .writeLock();

    private LogEntryEncoder                                                      logEntryEncoder;
    private LogEntryDecoder                                                      logEntryDecoder;

    public PMemLogStorage(final String uri, final RaftOptions raftOptions) {
        assert uri != null;
        this.path = uri;
    }

    @SuppressWarnings("unchecked")
    private PersistentSkipListMap2<PersistentLong, PersistentImmutableByteArray> createOrLoadDB(String id) {
        PersistentSkipListMap2<PersistentLong, PersistentImmutableByteArray> map = ObjectDirectory.get(id,
            PersistentSkipListMap2.class);
        if (map == null) {
            //map = new PersistentFPTree2<>(8, 64);
            map = new PersistentSkipListMap2<>();
            ObjectDirectory.put(id, map);
            this.firstLogIndex = DEFAULT_FIRST_LOG_INDEX;
            this.hasLoadFirstLogIndex = false;
            LOG.info("created PMem Log Storage {} {}", id, map.getClass().getSimpleName());
        } else {
            LOG.info("loaded PMem Log Storage {} {}", id, map.getClass().getSimpleName());
        }
        return map;
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        this.db = createOrLoadDB(this.path + "_persistent_log");
        this.confDb = createOrLoadDB(this.path + "_persistent_config");

        if (opts != null) {
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            load(opts.getConfigurationManager());
        }
        LOG.info("PMem Log Storage is initialized successfully");
        return true;
    }

    @Override
    public void shutdown() {
        onShutdown();
        this.db = null;
        this.confDb = null;
        this.hasLoadFirstLogIndex = false;
        LOG.info("PMem Log Storage is shutdown");
    }

    /**
     * First log index and last log index key in configuration db.
     */
    public static final Long FIRST_LOG_IDX_KEY = Long.MIN_VALUE;

    private void load(final ConfigurationManager confManager) {
        for (Map.Entry<PersistentLong, PersistentImmutableByteArray> e : this.confDb.entrySet()) {
            Long k = e.getKey().longValue();
            final LogEntry entry = this.logEntryDecoder.decode(e.getValue().toArray());
            if (k.equals(FIRST_LOG_IDX_KEY)) {
                setFirstLogIndex(entry.getId().getIndex());
                truncatePrefixInBackground(0, this.firstLogIndex);
            } else {
                assert (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
                final ConfigurationEntry confEntry = new ConfigurationEntry();
                confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                confEntry.setConf(new Configuration(entry.getPeers()));
                if (entry.getOldPeers() != null) {
                    confEntry.setOldConf(new Configuration(entry.getOldPeers()));
                }
                if (confManager != null) {
                    confManager.add(confEntry);
                }
            }
        }
    }

    private void setFirstLogIndex(final long index) {
        this.firstLogIndex = index;
        this.hasLoadFirstLogIndex = true;
    }

    /**
     * Save the first log index into conf DB.
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        LogEntry entry = new LogEntry(ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(firstLogIndex, 0L)); // term not used
        final byte[] content = this.logEntryEncoder.encode(entry);
        this.confDb.put(new PersistentLong(FIRST_LOG_IDX_KEY), new PersistentImmutableByteArray(content));
        return true;
    }

    @Override
    public long getFirstLogIndex() {
        if (this.hasLoadFirstLogIndex) {
            return this.firstLogIndex;
        }
        if (this.db.isEmpty()) {
            LOG.info("Get first log index from empty db where log index starts from {}", DEFAULT_FIRST_LOG_INDEX);
            return DEFAULT_FIRST_LOG_INDEX;
        }
        long logIndex = this.db.firstKey().longValue();
        saveFirstLogIndex(logIndex);
        setFirstLogIndex(logIndex);
        return logIndex;
    }

    @Override
    public long getLastLogIndex() {
        if (db.isEmpty()) {
            return 0L;
        }
        return db.lastKey().longValue();
    }

    @Override
    public LogEntry getEntry(final long index) {
        if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
            return null;
        }
        final PersistentLong pindex = new PersistentLong(index);
        if (!this.db.containsKey(pindex)) {
            return null;
        }
        return this.logEntryDecoder.decode(this.db.get(pindex).toArray());
    }

    // This is not suggested to use to get term.
    @Deprecated
    @Override
    public long getTerm(final long index) {
        if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
            return 0L;
        }
        final PersistentLong pindex = new PersistentLong(index);
        if (!this.db.containsKey(pindex)) {
            return 0L;
        }
        LogEntry entry = this.logEntryDecoder.decode(this.db.get(pindex).toArray());
        if (entry == null) {
            return 0L;
        }
        return entry.getId().getTerm();

    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        try {
            if (!validateConsecutive(entry)) {
                LOG.warn("Appending entries are not consecutive as last log index {}.", getLastLogIndex());
                truncateSuffix(entry.getId().getIndex() - 1);
                LOG.warn("Truncated suffix {}", entry.getId());
            }

            long index = entry.getId().getIndex();
            this.db
                .put(new PersistentLong(index), new PersistentImmutableByteArray(this.logEntryEncoder.encode(entry)));

            if (LOG.isDebugEnabled()) {
                LOG.debug("appended LogEntry {}", entry);
            }
            return true;
        } catch (Exception e) {
            LOG.error("failed to appendEntry {}, db.size={}", entry, this.db.size(), e);
            throw e;
        }
    }

    // return true if the entries.get(0).index == getLastLogIndex + 1
    // throw exception if entries itself is not consecutive
    private boolean validateConsecutive(final LogEntry entry) {
        if (entry == null) {
            throw new UnsupportedOperationException("Should never append null entry");
        }
        long currentIndex = entry.getId().getIndex();
        if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
            this.confDb.put(new PersistentLong(currentIndex),
                new PersistentImmutableByteArray(this.logEntryEncoder.encode(entry)));
        }
        return currentIndex == (getLastLogIndex() + 1);
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        try {
            if (!validateConsecutive(entries)) {
                LOG.warn("Appending entries are not consecutive as last log index {}.", getLastLogIndex());
                truncateSuffix(entries.get(0).getId().getIndex() - 1);
                LOG.warn("Truncated suffix {}", entries.get(0).getId());
            }

            if (!this.db.isEmpty()) {
                setFirstLogIndex(this.db.firstKey().longValue()); // to remove
            }
            for (LogEntry entry : entries) {
                this.db.put(new PersistentLong(entry.getId().getIndex()), new PersistentImmutableByteArray(
                    this.logEntryEncoder.encode(entry)));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("appended {} LogEntries. Last Log Index reaches {}", entries.size(), getLastLogIndex());
            }
            return entries.size();
        } catch (Exception e) {
            LOG.error("failed to appendEntries {}, db.size={}", entries.size(), this.db.size(), e);
            throw e;
        }
    }

    // validate entires, add conf into confDb if there is CONFIGURATION logEntry.
    // return true if the entries.get(0).index == getLastLogIndex + 1
    // throw exception if entries itself is not consecutive
    private boolean validateConsecutive(final List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return true;
        }
        LogEntry headEntry = entries.get(0);
        long lastIndex = headEntry.getId().getIndex() - 1;
        for (LogEntry entry : entries) {
            long currentIndex = entry.getId().getIndex();
            if (currentIndex != (lastIndex + 1)) {
                throw new UnsupportedOperationException((String.format(
                    "Non-consecutive log index appending attempted %d -> %d", lastIndex, currentIndex)));
            }
            if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                this.confDb.put(new PersistentLong(currentIndex),
                    new PersistentImmutableByteArray(this.logEntryEncoder.encode(entry)));
            }
            lastIndex = currentIndex;
        }
        return headEntry.getId().getIndex() == (getLastLogIndex() + 1);
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        if (this.hasLoadFirstLogIndex && firstIndexKept < this.firstLogIndex) {
            return false;
        }

        final long startIndex = getFirstLogIndex();
        final boolean ret = saveFirstLogIndex(firstIndexKept);
        if (ret) {
            setFirstLogIndex(firstIndexKept);
        }
        truncatePrefixInBackground(startIndex, firstIndexKept);

        return ret;
    }

    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        // delete logs in background.
        CompletableFuture.runAsync(() -> {
            this.writeLock.lock();
            try {
                if (this.db == null) {
                    return;
                }
                onTruncatePrefix(startIndex, firstIndexKept);
                for (long i = startIndex; i < firstIndexKept; i++) {
                    final PersistentLong index = new PersistentLong(i);
                    this.db.remove(index);
                    this.confDb.remove(index);
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("Log Entries [{} - {}] have been truncated", startIndex, firstIndexKept - 1);
                }
            } finally {
                this.writeLock.unlock();
            }
        });
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        onTruncateSuffix(lastIndexKept);
        for (long i = lastIndexKept + 1; i <= getLastLogIndex(); i++) {
            final PersistentLong index = new PersistentLong(i);
            this.db.remove(index);
            this.confDb.remove(index);
        }
        boolean success = getLastLogIndex() == lastIndexKept;
        if (!success) {
            LOG.warn("Failed to truncate suffix Log Entries [{},] potentially concurrent update", lastIndexKept + 1);
            //throw new NullPointerException("Failed to truncate suffix Log Entries " + (lastIndexKept+1));
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("truncated suffix Log Entries [{}+] successfully", lastIndexKept + 1);
        }
        return success;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        LogEntry entry = getEntry(nextLogIndex);
        destroyDB();
        onReset(nextLogIndex);
        if (init(null)) {
            if (entry == null) {
                entry = new LogEntry();
                entry.setType(ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(nextLogIndex, 0));
                LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
            }
            return appendEntry(entry);
        } else {
            return false;
        }
    }

    private void destroyDB() {
        this.db.clear();
        this.confDb.clear();
        this.db = null;
        this.confDb = null;
        this.firstLogIndex = DEFAULT_FIRST_LOG_INDEX;
        this.hasLoadFirstLogIndex = false;
    }

    protected boolean onInitLoaded() {
        return true;
    }

    protected void onShutdown() {
    }

    protected void onReset(final long nextLogIndex) {
    }

    protected void onTruncatePrefix(final long startIndex, final long firstIndexKept) {
    }

    protected void onSync() {
    }

    protected void onTruncateSuffix(final long lastIndexKept) {
    }

    protected LogEntry onDataAppend(final long logIndex, final LogEntry entry) {
        return entry;
    }

    protected LogEntry onDataGet(final long logIndex, final LogEntry entry) {
        return entry;
    }

    @Override
    public void describe(final Printer out) {
        StringBuilder sb = new StringBuilder();
        sb.append("PMemLogStorage").append("\r\n").append("FirstLogIndex        : ").append(firstLogIndex)
            .append("\r\n").append("HasLoadFirstLogIndex : ").append(hasLoadFirstLogIndex).append("\r\n")
            .append("LastLogIndex         : ").append(getLastLogIndex()).append("\r\n")
            .append("LogEntries Size      : ").append(this.db.size()).append("\r\n");
        out.print(sb.toString());
    }
}
