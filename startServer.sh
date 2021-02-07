sudo mount -o dax /dev/pmem1 /mnt/mem/
sudo mount -o dax /dev/pmem0 /mnt/mem0/
sudo rm -rf /mnt/mem/*
sudo rm -rf /mnt/mem0/*
sudo systemctl stop SuSEfirewall2.service
sudo systemctl disable SuSEfirewall2.service
sudo /sbin/SuSEfirewall2 off
sudo rm ./output
#export LD_PRELOAD=/usr/local/lib64/libtcmalloc.so
#export HEAPPROFILE=/tmp/perf-result/
#export HEAP_PROFILE_ALLOCATION_INTERVAL=2000000000
#export MAVEN_OPTS="-XX:NativeMemoryTracking=summary -Xms6g -Xmx6g -Xmn3g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=10g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./log/blobsvc-$$.hprof -XX:+UnlockDiagnosticVMOptions -XX:GuaranteedSafepointInterval=30000 -Djdk.nio.maxCachedBufferSize=1048576  -XX:-UseBiasedLocking -Dsun.net.inetaddr.ttl=0 -XX:MaxTenuringThreshold=8 -XX:MaxPermSize=256m -XX:+UseConcMarkSweepGC -XX:+ExplicitGCInvokesConcurrent -XX:+CMSScavengeBeforeRemark -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:+ExitOnOutOfMemoryError -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCApplicationStoppedTime -XX:+PrintTenuringDistribution -Xloggc:./log/test-gc-$((($(ls ./log/test-gc-*.log -t 2>/dev/null | head -n1 | sed 's/.*-gc-\([0-9]\).*/\1/')+1)%10)).log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 -XX:GCLogFileSize=50M"

mvn -pl jraft-example exec:java  -Dexec.mainClass=com.alipay.sofa.jraft.benchmark.server.BenchmarkServer  -Dexec.classpathScope=test -Djraft.available_processors=10 -Dbolt.tcp.so.sndbuf=1048576 -Dbolt.tcp.so.rcvbuf=1048576 -Dexec.args="server 10.255.93.15:18091,10.255.93.16:18091,10.255.93.17:18091 jraft-example/config/benchmark_server_pmem.yaml" > output &
tailf output
