package com.alipay.sofa.jraft.rhea.benchmark.raw;

import com.alipay.sofa.jraft.rhea.options.MemoryDBOptions;
import com.alipay.sofa.jraft.rhea.storage.MemoryRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;

public class MemoryRawKVGetBenchmark extends RawKVGetBenchmark {
    private MemoryRawKVStore memoryRawKVStore;

    @Override
    protected RawKVStore initRawKVStore() {
        this.memoryRawKVStore = new MemoryRawKVStore();
        this.memoryRawKVStore.init(new MemoryDBOptions());
        return memoryRawKVStore;
    }

    @Override
    protected RawKVStore rawKVStore(){
        return this.memoryRawKVStore;
    }

    @Override
    protected void shutdown() {
        this.memoryRawKVStore.shutdown();
    }
}
