package com.alipay.sofa.jraft.rhea.benchmark.raw;

import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.storage.PMemRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;

public class PMemRawKVGetBenchmark extends RawKVGetBenchmark {
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
}
