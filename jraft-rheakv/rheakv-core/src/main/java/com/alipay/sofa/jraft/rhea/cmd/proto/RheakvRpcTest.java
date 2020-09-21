/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rhea.cmd.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 * @author baozi
 * @date 2020/6/4 9:00 PM
 */
public class RheakvRpcTest {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        final RheakvRpc.GetAndPutRequest getAndPutRequest = RheakvRpc.GetAndPutRequest.newBuilder()
            .setKey(ByteString.copyFrom(writeUtf8("getAndPut"))).setValue(ByteString.copyFrom(writeUtf8("getAndPut")))
            .build();

        final RheakvRpc.BaseRequest request = RheakvRpc.BaseRequest.newBuilder().setRegionId(1L).setConfVer(1l)
            .setVersion(1l).setRequestType(RheakvRpc.BaseRequest.RequestType.getAndPut)
            .setExtension(RheakvRpc.GetAndPutRequest.body, getAndPutRequest).build();

        System.out.println("*****before serialize=" + request);

        byte[] bytes = request.toByteArray();

        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        registry.add(RheakvRpc.GetRequest.body);
        registry.add(RheakvRpc.GetAndPutRequest.body);

        RheakvRpc.BaseRequest baseRequest = RheakvRpc.BaseRequest.parseFrom(bytes, registry);
        System.out.println("*****after deserialize");

        RheakvRpc.GetAndPutRequest getAndPutRequestD2 = baseRequest.getExtension(RheakvRpc.GetAndPutRequest.body);
        System.out.println(readUtf8(getAndPutRequestD2.getKey().toByteArray()));
    }
}
