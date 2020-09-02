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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.DebugStatistics;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Log storage in persistent memory by pmemobj vector
 *
 * @author Qinglin Mao (Qinglin_Mao@Dell.com)
 *
 * 2020-Aug-08 13:50:00 PM
 */
public class PMemVectorLogStorage implements LogStorage, Describer {

    private static final Logger LOG                     = LoggerFactory.getLogger(PMemVectorLogStorage.class);

    private static final long   DEFAULT_FIRST_LOG_INDEX = 1L;

    private final String        path;
    private long                pointer                 = 0;
    private final ReadWriteLock readWriteLock           = new ReentrantReadWriteLock();
    private final Lock          readLock                = this.readWriteLock.readLock();
    private final Lock          writeLock               = this.readWriteLock.writeLock();

    private volatile long       firstLogIndex           = DEFAULT_FIRST_LOG_INDEX;

    private volatile boolean    hasLoadFirstLogIndex    = false;

    private LogEntryEncoder     logEntryEncoder;
    private LogEntryDecoder     logEntryDecoder;

    public PMemVectorLogStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.writeLock.lock();
        try {
            if (this.pointer != 0) {
                LOG.warn("PMemVectorLogStorage init() already.");
                return true;
            }
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");

            this.pointer = pmem_log_open(this.path, 64 * 1024 * 1024 /* TODO: pool size  */);
            Requires.requireTrue(this.pointer != 0);
            this.firstLogIndex = DEFAULT_FIRST_LOG_INDEX;
            this.hasLoadFirstLogIndex = false;
            // load first log index from pmem
            getFirstLogIndex();
            loadConf(opts.getConfigurationManager());
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    // TODO: improve
    private void loadConf(final ConfigurationManager confManager) {
        checkState();
        final long size = pmem_log_size(this.pointer);
        for (int index = 0; index < size; ++index) {
            byte[] buf = pmem_log_get(this.pointer, index);
            Requires.requireNonNull(buf);

            final LogEntry entry = this.logEntryDecoder.decode(buf);
            Requires.requireNonNull(entry, "Fail to decode conf entry at index " + (getFirstLogIndex() + index)
                                           + ", the log data is: " + BytesUtil.toHex(buf));
            if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                final ConfigurationEntry confEntry = new ConfigurationEntry();
                confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                confEntry.setConf(new Configuration(entry.getPeers(), entry.getLearners()));
                if (entry.getOldPeers() != null) {
                    confEntry.setOldConf(new Configuration(entry.getOldPeers(), entry.getOldLearners()));
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

    private void checkState() {
        Requires.requireNonNull(this.pointer, "pmem log not initialized or destroyed");
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        pmem_log_close(this.pointer);
        this.pointer = 0;
        this.firstLogIndex = DEFAULT_FIRST_LOG_INDEX;
        this.hasLoadFirstLogIndex = false;
        LOG.info("PMem Log Storage {} is shutdown", this.path);
        this.writeLock.unlock();
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            checkState();

            if (pmem_log_empty(this.pointer)) {
                LOG.info("Get first log index from empty pmem log where log index starts from {}",
                    DEFAULT_FIRST_LOG_INDEX);
                return DEFAULT_FIRST_LOG_INDEX;
            }

            byte[] buf = pmem_log_get(this.pointer, 0);
            Requires.requireNonNull(buf);
            final LogEntry entry = this.logEntryDecoder.decode(buf);
            Requires.requireNonNull(entry);
            setFirstLogIndex(entry.getId().getIndex());
            return entry.getId().getIndex();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        checkState();

        try {
            if (pmem_log_empty(this.pointer)) {
                LOG.info("Get last log index from empty pmem log where log index starts from {}",
                    DEFAULT_FIRST_LOG_INDEX);
                return 0L;
            }

            byte[] buf = pmem_log_get(this.pointer, pmem_log_size(this.pointer) - 1);
            Requires.requireNonNull(buf);
            final LogEntry entry = this.logEntryDecoder.decode(buf);
            Requires.requireNonNull(entry);
            return entry.getId().getIndex();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }

            if (pmem_log_empty(this.pointer)) {
                return null;
            }

            byte[] buf = pmem_log_get(this.pointer, index - getFirstLogIndex());
            Requires.requireNonNull(buf);
            final LogEntry entry = this.logEntryDecoder.decode(buf);
            Requires.requireNonNull(entry);
            return entry;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getTerm(final long index) {
        final LogEntry entry = getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        this.writeLock.lock();
        checkState();
        final byte[] buf = this.logEntryEncoder.encode(entry);
        pmem_log_append(this.pointer, buf);
        this.writeLock.unlock();
        return true;
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        this.writeLock.lock();
        try {
            if (entries == null || entries.isEmpty()) {
                return 0;
            }

            for (LogEntry entry : entries) {
                appendEntry(entry);
            }

            return entries.size();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        this.writeLock.lock();
        final long startIndex = getFirstLogIndex();
        pmem_log_truncate_prefix(this.pointer, firstIndexKept - startIndex);
        Requires.requireTrue(getFirstLogIndex() == firstIndexKept, "Unexpected first log index, actual is "
                                                                   + getFirstLogIndex() + ", expected is "
                                                                   + firstIndexKept);
        setFirstLogIndex(firstIndexKept);
        this.writeLock.unlock();
        return true;
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.writeLock.lock();
        final long lastIndex = getLastLogIndex();
        pmem_log_truncate_suffix(this.pointer, lastIndex - lastIndexKept);
        this.writeLock.unlock();
        return true;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        LogEntry entry = getEntry(nextLogIndex);
        pmem_log_clear(this.pointer);
        pmem_log_close(this.pointer);
        this.pointer = 0;
        try {
            if (init(null)) {
                if (entry == null) {
                    entry = new LogEntry();
                    entry.setType(EntryType.ENTRY_TYPE_NO_OP);
                    entry.setId(new LogId(nextLogIndex, 0));
                    LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
                }
                return appendEntry(entry);
            } else {
                return false;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        this.readLock.lock();
        StringBuilder sb = new StringBuilder();
        sb.append("PMemVectorLogStorage").append("\r\n").append("FirstLogIndex        : ").append(this.firstLogIndex)
            .append("\r\n").append("HasLoadFirstLogIndex : ").append(this.hasLoadFirstLogIndex).append("\r\n")
            .append("LastLogIndex         : ").append(getLastLogIndex()).append("\r\n")
            .append("LogEntries Size      : ").append(pmem_log_size(this.pointer)).append("\r\n");
        out.print(sb.toString());

        this.readLock.unlock();
    }

    // JNI METHODS
    // --------------------------------------------------------------------------------
    private native long pmem_log_open(String path, long pool_size);

    private native void pmem_log_close(long ptr);

    private native void pmem_log_append(long ptr, byte[] buf);

    private native byte[] pmem_log_get(long ptr, long index);

    private native boolean pmem_log_empty(long ptr);

    private native long pmem_log_size(long ptr);

    private native void pmem_log_truncate_prefix(long ptr, long num);

    private native void pmem_log_truncate_suffix(long ptr, long num);

    private native void pmem_log_clear(long ptr);

    static {
        System.loadLibrary("pmemlog-jni");
    }
}
