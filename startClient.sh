sudo systemctl stop SuSEfirewall2.service
sudo systemctl disable SuSEfirewall2.service
sudo /sbin/SuSEfirewall2 off
echo "$1"

mvn -pl jraft-example exec:java -Dexec.mainClass=com.alipay.sofa.jraft.benchmark.client.BenchmarkClient -Dexec.classpathScope=test  -Dexec.args="client 10.255.93.15:18091,10.255.93.16:18091,10.255.93.17:18091 jraft-example/config/benchmark_client_pmem_128.yaml $1 10 0 10000000 64 1024" > client_output& 
#mvn -pl jraft-example exec:java -Dexec.mainClass=com.alipay.sofa.jraft.benchmark.server.BenchmarkServer -Dexec.classpathScope=test  -Dexec.args="-Xms6g -Xmx6g -Xmn3g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=16g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC -cp jraft-example/target/jraft-bin/lib/*.jar  server 10.255.93.15:18091,10.255.93.16:18091,10.255.93.17:18091 jraft-example/config/benchmark_server_pmem.yaml true $1 10 0 1000000 64 1024"
#tailf output
tailf  client_output 
