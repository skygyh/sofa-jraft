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

import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.serialization.JavaSerializer;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author baozi
 * @date 2020/6/6 5:25 PM
 */
public class TestErrorsResponse {

    public static void main(String[] args) throws InvalidProtocolBufferException {
        RheakvRpc.BaseResponse response = RheakvRpc.BaseResponse.newBuilder().setRegionId(1)
            .setError(JavaSerializer.serializeByteString(Errors.NO_REGION_FOUND))
            .setResponseType(RheakvRpc.BaseResponse.ResponseType.noRegionFund)
            .setValue(JavaSerializer.serializeByteString(false)).build();

        byte[] bytes = response.toByteArray();
        RheakvRpc.BaseResponse responseDe = RheakvRpc.BaseResponse.parseFrom(bytes);
        System.out.println(responseDe);

        Errors errors = (Errors) JavaSerializer.deserialize(responseDe.getError().toByteArray());
        System.out.println("----de errors----");
        System.out.println(errors.message());
        System.out.println(errors.isSuccess());
        System.out.println(errors.code());
        System.out.println(errors.exceptionName());
        System.out.println(errors.exception());

    }
}
