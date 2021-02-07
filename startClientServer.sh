sudo mount -o dax /dev/pmem1 /mnt/mem/
sudo mount -o dax /dev/pmem0 /mnt/mem0/
sudo rm -rf /mnt/mem/*
sudo rm -rf /mnt/mem0/*
sudo systemctl stop SuSEfirewall2.service
sudo systemctl disable SuSEfirewall2.service
sudo /sbin/SuSEfirewall2 off
sudo rm ./output
echo "$1"

mvn -pl jraft-example exec:java -Dexec.mainClass=com.alipay.sofa.jraft.benchmark.server.BenchmarkServer -Dexec.classpathScope=test -Djraft.available_processors=10 -Dbolt.tcp.so.sndbuf=1048576 -Dbolt.tcp.so.rcvbuf=1048576 -Dexec.args="server 10.255.93.15:18091,10.255.93.16:18091,10.255.93.17:18091 jraft-example/config/benchmark_server_pmem.yaml true $1 10 0 10000000 64 1024" > ./output&
#mvn -pl jraft-example exec:java -Dexec.mainClass=com.alipay.sofa.jraft.benchmark.server.BenchmarkServer -Dexec.classpathScope=test  -Dexec.args="-Xms6g -Xmx6g -Xmn3g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=16g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC -cp jraft-example/target/jraft-bin/lib/*.jar  server 10.255.93.15:18091,10.255.93.16:18091,10.255.93.17:18091 jraft-example/config/benchmark_server_pmem.yaml true $1 10 0 1000000 64 1024"
#tailf output
tailf  output  | grep -A 9 put_benchmark_timer
