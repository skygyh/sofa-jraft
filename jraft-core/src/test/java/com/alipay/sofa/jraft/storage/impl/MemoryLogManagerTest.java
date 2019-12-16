package com.alipay.sofa.jraft.storage.impl;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;

public class MemoryLogManagerTest extends LogManagerTest {
    @Override
    public LogStorage newLogStorage(final RaftOptions raftOptions) {
        return new MemoryLogStorage(raftOptions);
    }
}
