package com.alipay.sofa.jraft.rhea.benchmark.raw;

import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.storage.PMemRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public class PMemRawKVPutBenchmark extends RawKVPutBenchmark {
    private PMemRawKVStore pmemRawKVStore;

    @Override
    protected RawKVStore initRawKVStore() {
        this.pmemRawKVStore = new PMemRawKVStore();
        this.pmemRawKVStore.init(new PMemDBOptions());
        return this.pmemRawKVStore;
    }

    @Override
    protected RawKVStore rawKVStore(){
        return this.pmemRawKVStore;
    }

    @Override
    protected void shutdown() {
        this.pmemRawKVStore.shutdown();
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
