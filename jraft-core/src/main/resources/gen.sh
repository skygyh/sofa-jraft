#!/bin/bash
java -jar ~/opensources/protoc-jar-351.jar --java_shaded_out=../java/  -I=./  --descriptor_set_out=raft.desc enum.proto local_file_meta.proto raft.proto local_storage.proto rpc.proto cli.proto log.proto
