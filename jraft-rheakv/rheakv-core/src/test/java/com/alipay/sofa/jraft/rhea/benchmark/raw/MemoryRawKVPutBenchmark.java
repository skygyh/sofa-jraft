package com.alipay.sofa.jraft.rhea.benchmark.raw;

import com.alipay.sofa.jraft.rhea.options.MemoryDBOptions;
import com.alipay.sofa.jraft.rhea.storage.MemoryRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public class MemoryRawKVPutBenchmark extends RawKVPutBenchmark {
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

    @Setup
    public void setup() {
        try {
            super.setup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @TearDown
    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
