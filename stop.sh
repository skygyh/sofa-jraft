kill `ps -elf | grep java | grep com.alipay.sofa.jraft.benchmark.server.BenchmarkServer | awk '{print $4}'`
kill `ps -elf | grep java | grep com.alipay.sofa.jraft.benchmark.client.BenchmarkClient | awk '{print $4}'`
