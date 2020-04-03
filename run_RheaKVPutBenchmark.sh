#!/bin/bash
export PMEM_IS_PMEM_FORCE=1
export PMEMOBJ_CONF="sds.at_create=0"
export DB_PATH="/mnt/mem/benchmark_rhea_db/"
export RAFT_PATH="/mnt/mem/benchmark_rhea_raft/"
rm -rf $DB_PATH
rm -rf $RAFT_PATH
export HOME="/home/`whoami`"
export VERSION=`awk -F '[<>]' '/<version>/{print $3}' pom.xml | head -n 1`
export BOLTVER=`awk -F '[<>]' '/<bolt.version>/{print $3}' pom.xml | head -n 1`
export DATABIND_VER=`awk -F '[<>]' '/<jackson.databind.version>/{print $3}' jraft-rheakv/rheakv-core/pom.xml | head -n 1` # 2.9.10.1
export DATAFORMAT_VER=`awk -F '[<>]' '/<jackson.dataformat.version>/{print $3}' jraft-rheakv/rheakv-core/pom.xml | head -n 1`
export NETTY_VER=4.1.42
echo "HOME=$HOME"
pushd `pwd`
cd jraft-rheakv/rheakv-core/target/test-classes
java -cp "../../../rheakv-core/target/classes:../../../rheakv-core/target/test-classes/:../../../rheakv-pd/target/classes/:../../../../pcj/target/classes/:$HOME/.m2/repository/:$HOME/opensources/sofa-jraft/jraft-core/target/classes/:$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-core/$DATAFORMAT_VER/jackson-core-$DATAFORMAT_VER.jar:$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-databind/$DATABIND_VER/jackson-databind-$DATABIND_VER.jar:$HOME/.m2/repository/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/$DATAFORMAT_VER/jackson-dataformat-yaml-$DATAFORMAT_VER.jar:../../../rheakv-pd/target/jraft-rheakv-pd-$VERSION.jar:../../../rheakv-core/target/jraft-rheakv-core-$VERSION.jar:../../../../jraft-core/target/jraft-core-$VERSION.jar:$HOME/.m2/repository/org/openjdk/jmh/jmh-core/1.20/jmh-core-1.20.jar:$HOME/.m2/repository/org/slf4j/slf4j-api/1.7.21/slf4j-api-1.7.21.jar:$HOME/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar:$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/$DATAFORMAT_VER/jackson-annotations-$DATAFORMAT_VER.jar:$HOME/.m2/repository/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/$DATAFORMAT_VER/jackson-dataformat-yaml-$DATAFORMAT_VER.jar:$HOME/.m2/repository/org/yaml/snakeyaml/1.23/snakeyaml-1.23.jar:$HOME/.m2/repository/io/dropwizard/metrics/metrics-core/4.0.2/metrics-core-4.0.2.jar:$HOME/.m2/repository/org/apache/logging/log4j/log4j-slf4j-impl/2.2/log4j-slf4j-impl-2.2.jar:$HOME/.m2/repository/com/alipay/sofa/jraft-rheakv-core/$VERSION/jraft-rheakv-core-$VERSION.jar:$HOME/.m2/repository/org/apache/logging/log4j/log4j-api/2.2/log4j-api-2.2.jar:$HOME/.m2/repository/com/lmax/disruptor/3.3.7/disruptor-3.3.7.jar:$HOME/.m2/repository/org/apache/logging/log4j/log4j-core/2.2/log4j-core-2.2.jar:$HOME/.m2/repository/com/alipay/sofa/hessian/3.3.6/hessian-3.3.6.jar:$HOME/.m2/repository/com/google/protobuf/protobuf-java/3.5.1/protobuf-java-3.5.1.jar:$HOME/.m2/repository/commons-lang/commons-lang/2.6/commons-lang-2.6.jar:$HOME/.m2/repository/io/netty/netty-all/${NETTY_VER}.Final/netty-all-${NETTY_VER}.Final.jar:$HOME/.m2/repository/com/alipay/sofa/common/sofa-common-tools/1.0.12/sofa-common-tools-1.0.12.jar:$HOME/.m2/repository/org/rocksdb/rocksdbjni/5.18.3/rocksdbjni-5.18.3.jar:$HOME/.m2/repository/org/jctools/jctools-core/2.1.1/jctools-core-2.1.1.jar:$HOME/.m2/repository/io/protostuff/protostuff-runtime/1.6.0/protostuff-runtime-1.6.0.jar:$HOME/.m2/repository/io/protostuff/protostuff-api/1.6.0/protostuff-api-1.6.0.jar:$HOME/.m2/repository/io/protostuff/protostuff-collectionschema/1.6.0/protostuff-collectionschema-1.6.0.jar:$HOME/.m2/repository/io/protostuff/protostuff-core/1.6.0/protostuff-core-1.6.0.jar:$HOME/.m2/repository/org/apache/commons/commons-math3/3.2/commons-math3-3.2.jar:$HOME/.m2/repository/com/alipay/sofa/bolt/$BOLTVER/bolt-$BOLTVER.jar:../../../rheakv-core/libs/pmemkv-1.1.0.jar:./" -Djava.library.path=/usr/local/lib com.alipay.sofa.jraft.rhea.benchmark.rhea.RheaKVPutBenchmark $@
popd
