package com.alipay.sofa.jraft.rhea.benchmark.raw;

import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;

public class RocksRawKVPutBenchmark extends RawKVPutBenchmark {
    protected String          tempPath;
    protected RocksRawKVStore rocksRawKVStore;
    protected RocksDBOptions dbOptions;

    @Override
    protected RawKVStore initRawKVStore() throws IOException{
        File file = getTempDir();
        this.tempPath = file.getAbsolutePath();
        System.out.println(this.tempPath);
        this.rocksRawKVStore = new RocksRawKVStore();
        this.dbOptions = new RocksDBOptions();
        this.dbOptions.setDbPath(this.tempPath);
        this.dbOptions.setSync(false);
        this.rocksRawKVStore.init(this.dbOptions);
        return this.rocksRawKVStore;
    }

    @Override
    protected RawKVStore rawKVStore(){
        return this.rocksRawKVStore;
    }

    @Override
    protected void shutdown() throws IOException {
        this.rocksRawKVStore.shutdown();
        FileUtils.deleteQuietly(new File((this.tempPath)));
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
