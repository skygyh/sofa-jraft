#!/usr/bin/bash
rm -rf /mnt/mem/*
mvn --projects jraft-core test -Dtest=com.alipay.sofa.jraft.storage.impl.PMemLogStorageTest
