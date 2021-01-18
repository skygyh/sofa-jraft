#! /bin/bash
sudo mount -o dax /dev/pmem1 /mnt/mem/
sudo rm -rf /mnt/mem/*
sudo systemctl stop SuSEfirewall2.service
sudo systemctl disable SuSEfirewall2.service
sudo /sbin/SuSEfirewall2 off

BASE_DIR=$(dirname $0)/..
CLASSPATH=$(echo $BASE_DIR/lib/*.jar | tr ' ' ':')

# get java version
JAVA="$JAVA_HOME/bin/java"

JAVA_VERSION=$($JAVA -version 2>&1 | awk -F\" '/java version/{print $2}')
echo "java version:$JAVA_VERSION path:$JAVA"

MEMORY=$(cat /proc/meminfo |grep 'MemTotal' |awk -F : '{print $2}' |awk '{print $1}' |sed 's/^[ \t]*//g')
echo -e "Total Memory:\n${MEMORY} KB\n"

if [[ $JAVA_VERSION =~ "1.8" ]]; then
  echo "Use java version 1.8 opt"

  if (($MEMORY <= 5000000));then
    JAVA_OPT_1="-server -Xms2000m -Xmx2000m -Xmn1000m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=1024m "
  elif (($MEMORY <= 9000000));then
    JAVA_OPT_1="-server -Xms3500m -Xmx3500m -Xmn1500m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=1536m "
  elif (($MEMORY <= 17000000));then
    JAVA_OPT_1="-server -Xms4g -Xmx4g -Xmn2g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=4g "
  elif (($MEMORY <= 33000000));then
    JAVA_OPT_1="-server -Xms5g -Xmx5g -Xmn2g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=8g "
  else
    JAVA_OPT_1="-server -Xms6g -Xmx6g -Xmn3g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=16g "
  fi

#  JAVA_OPT_2="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC"
  JAVA_OPT_2="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./log/blobsvc-$$.hprof -XX:+UnlockDiagnosticVMOptions -XX:GuaranteedSafepointInterval=30000 -Djdk.nio.maxCachedBufferSize=1048576  -XX:-UseBiasedLocking -Dsun.net.inetaddr.ttl=0 -XX:MaxTenuringThreshold=8 -XX:MaxPermSize=256m -XX:+UseConcMarkSweepGC -XX:+ExplicitGCInvokesConcurrent -XX:+CMSScavengeBeforeRemark -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:+ExitOnOutOfMemoryError -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCApplicationStoppedTime -XX:+PrintTenuringDistribution -Xloggc:./log/gc-$((($(ls ./log/gc-*.log.0* -t 2>/dev/null | head -n1 | sed 's/.*-gc-\([0-9]\).*/\1/')+1)%10)).log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 -XX:GCLogFileSize=50M"

else
  echo "Error, not support java version : $JAVA_VERSION"
  exit 1
fi

#example: ./server_benchmark_start.sh  10.255.93.15:18091,10.255.93.16:18091,10.255.93.17:18091 ../config/benchmark_server_pmem.yaml false
JAVA_OPTS="${JAVA_OPT_1} ${JAVA_OPT_2}"

JAVA_CONFIG=$(mktemp XXXXXXXX)
cat <<EOF | xargs echo > $JAVA_CONFIG
${JAVA_OPTS}
-cp $CLASSPATH
com.alipay.sofa.jraft.benchmark.BenchmarkBootstrap
server
$1
$2
$3
EOF

JAVA_CONFIG=$(cat $JAVA_CONFIG | xargs echo)

echo $JAVA_CONFIG

JAVA="$JAVA_HOME/bin/java"

HOSTNAME=`hostname`

nohup $JAVA $JAVA_CONFIG >> server_stdout 2>&1 &

tailf server_stdout
