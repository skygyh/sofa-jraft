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

import com.google.protobuf.Extension;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author baozi
 * @date 2020/5/31 5:03 PM
 */
public class TestProto {

    public static void main(String[] args) throws InvalidProtocolBufferException {
        RheakvRpcT1.ChatMessage deserializedMessage = RheakvRpcT1.ChatMessage
            .newBuilder()
            .setId("123")
            .setFrom("100@pingan.com.cn")
            .setTo("101@pingan.com.cn")
            .setType(RheakvRpcT1.ChatMessage.Type.chat)
            .setContentType(RheakvRpcT1.ChatMessage.ContentType.text)
            .setExtension(RheakvRpcT1.TextBody.body,
                RheakvRpcT1.TextBody.newBuilder().setContent("hello world").build()).build();

        //        System.out.println("*****before serialize=" + textMessage);
        //
        //        byte[] bytes = textMessage.toByteArray();
        //
        //        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        //        registry.add(RheakvRpcT1.ImageBody.body);
        //        registry.add(RheakvRpcT1.TextBody.body);
        //
        //        RheakvRpcT1.ChatMessage deserializedMessage = RheakvRpcT1.ChatMessage.parseFrom(bytes, registry);
        //        System.out.println("*****after deserialize");

        Map<RheakvRpcT1.ChatMessage.ContentType, Extension<RheakvRpcT1.ChatMessage, ?>> contentType2ExtensionBody = new HashMap<RheakvRpcT1.ChatMessage.ContentType, Extension<RheakvRpcT1.ChatMessage, ?>>();
        contentType2ExtensionBody.put(RheakvRpcT1.ChatMessage.ContentType.text, RheakvRpcT1.TextBody.body);
        contentType2ExtensionBody.put(RheakvRpcT1.ChatMessage.ContentType.image, RheakvRpcT1.ImageBody.body);

        RheakvRpcT1.ChatMessage.ContentType contentType = deserializedMessage.getContentType();
        switch (contentType) {
            case text:
                RheakvRpcT1.TextBody deserializedTextBody = (RheakvRpcT1.TextBody) deserializedMessage
                    .getExtension(contentType2ExtensionBody.get(contentType));
                System.out.println("body=" + deserializedTextBody);
                break;
            case image:
                RheakvRpcT1.ImageBody deserializedImageBody = (RheakvRpcT1.ImageBody) deserializedMessage
                    .getExtension(contentType2ExtensionBody.get(contentType));
                System.out.println("body=" + deserializedImageBody);
                break;
            default:
                break;
        }
    }
}
