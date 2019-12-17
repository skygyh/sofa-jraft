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
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Describer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alipay.sofa.jraft.entity.EnumOutter.EntryType.ENTRY_TYPE_NO_OP;

/**
 * Log storage in volatile memory.
 *
 * @author Jerry Yang (20830326@qq.com)
 *
 * 2019-Dec-11 11:16:06 AM
 */
public class MemoryLogStorage implements LogStorage, Describer {
    private static final Logger                   LOG                     = LoggerFactory
                                                                              .getLogger(MemoryLogStorage.class);
    private static final long                     DEFAULT_FIRST_LOG_INDEX = 1L;

    private ConcurrentSkipListMap<Long, LogEntry> db                      = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Long, LogEntry> confDb                  = new ConcurrentSkipListMap<>();
    private volatile long                         firstLogIndex           = DEFAULT_FIRST_LOG_INDEX;
    private volatile boolean                      hasLoadFirstLogIndex    = false;
    private final ReadWriteLock                   readWriteLock           = new ReentrantReadWriteLock();
    private final Lock                            writeLock               = this.readWriteLock.writeLock();

    public MemoryLogStorage(final RaftOptions raftOptions) {

    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        if (this.db == null) {
            this.db = new ConcurrentSkipListMap<>();
            this.firstLogIndex = DEFAULT_FIRST_LOG_INDEX;
            this.hasLoadFirstLogIndex = false;
            this.confDb = new ConcurrentSkipListMap<>();

        }
        if (opts != null) {
            load(opts.getConfigurationManager());
        }
        LOG.info("Memory Log Storage is initialized successfully");
        return true;
    }

    @Override
    public void shutdown() {
        LOG.info("Memory Log Storage is shutdown");
    }

    /**
     * First log index and last log index key in configuration db.
     */
    public static final Long FIRST_LOG_IDX_KEY = Long.MIN_VALUE;

    private void load(final ConfigurationManager confManager) {
        for (Map.Entry<Long, LogEntry> e : this.confDb.entrySet()) {
            Long k = e.getKey();
            LogEntry entry = e.getValue();
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
        this.confDb.put(FIRST_LOG_IDX_KEY, entry);
        return true;
    }

    @Override
    public long getFirstLogIndex() {
        if (this.hasLoadFirstLogIndex) {
            return this.firstLogIndex;
        }
        if (!this.db.isEmpty()) {
            long logIndex = this.db.firstKey();
            saveFirstLogIndex(logIndex);
            setFirstLogIndex(logIndex);
        }
        return DEFAULT_FIRST_LOG_INDEX;
    }

    @Override
    public long getLastLogIndex() {
        if (db.isEmpty()) {
            return 0L;
        }
        return db.lastKey();
    }

    @Override
    public LogEntry getEntry(final long index) {
        if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
            return null;
        }
        return this.db.get(index);
    }

    @Deprecated
    @Override
    public long getTerm(final long index) {
        if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
            return 0L;
        }
        LogEntry entry = this.db.get(index);
        if (entry == null) {
            return 0L;
        }
        return entry.getId().getTerm();

    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        if (!validateConsecutive(entry)) {
            LOG.warn("Appending entries are not consecutive as last log index {}.", getLastLogIndex());
            truncateSuffix(entry.getId().getIndex() - 1);
            LOG.warn("Truncated suffix {}", entry.getId());
        }

        long index = entry.getId().getIndex();
        this.db.put(index, entry);
        return true;
    }

    // return true if the entries.get(0).index == getLastLogIndex + 1
    // throw exception if entries itself is not consecutive
    private boolean validateConsecutive(final LogEntry entry) {
        if (entry == null) {
            throw new UnsupportedOperationException("Should never append null entry");
        }
        long currentIndex = entry.getId().getIndex();
        if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
            this.confDb.put(currentIndex, entry);
        }
        return currentIndex == (getLastLogIndex() + 1);
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        if (!validateConsecutive(entries)) {
            LOG.warn("Appending entries are not consecutive as last log index {}.", getLastLogIndex());
            truncateSuffix(entries.get(0).getId().getIndex() - 1);
            LOG.warn("Truncated suffix {}", entries.get(0).getId());
        }

        if (!this.db.isEmpty()) {
            setFirstLogIndex(this.db.firstKey()); // to remove
        }
        for (LogEntry entry : entries) {
            this.db.put(entry.getId().getIndex(), entry);
        }
        return entries.size();
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
                this.confDb.put(currentIndex, entry);
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
                    this.db.remove(i);
                    this.confDb.remove(i);
                }
            } finally {
                this.writeLock.unlock();
            }
        });
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        onTruncateSuffix(lastIndexKept);
        for (Long i = lastIndexKept + 1; i < getLastLogIndex(); i++) {
            this.db.remove(i);
            this.confDb.remove(i);
        }
        return false;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        LogEntry entry = getEntry(nextLogIndex);
        destroyDB();
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
        this.db = null;
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
        sb.append("MemoryLogStorage").append("\r\n").append("FirstLogIndex        : ").append(firstLogIndex)
            .append("\r\n").append("HasLoadFirstLogIndex : ").append(hasLoadFirstLogIndex).append("\r\n")
            .append("LastLogIndex         : ").append(getLastLogIndex()).append("\r\n")
            .append("LogEntries Size      : ").append(this.db.size()).append("\r\n");
        out.print(sb.toString());
    }
}
