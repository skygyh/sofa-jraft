sudo mount -o dax /dev/pmem1 /mnt/mem/
sudo rm -rf /mnt/mem/*
sudo systemctl stop SuSEfirewall2.service
sudo systemctl disable SuSEfirewall2.service
sudo /sbin/SuSEfirewall2 off
mvn -pl jraft-example exec:java  -Dexec.mainClass=com.alipay.sofa.jraft.benchmark.server.BenchmarkServer -Dexec.classpathScope=test  -Dexec.args="server 10.255.93.15:18091,10.255.93.16:18091,10.255.93.17:18091 jraft-example/config/benchmark_server_pmem.yaml" > output &
tailf output
