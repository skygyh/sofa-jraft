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
package com.alipay.sofa.jraft.rhea.cmd.store;

import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

import java.util.List;

/**
 *
 * @author Jerry Yang
 */
public class BatchCompositeRequest extends BaseRequest {

    private static final long serialVersionUID = -980036845124180958L;

    private List<BaseRequest> compositeRequests;

    public BatchCompositeRequest() {
    }

    public BatchCompositeRequest(List<BaseRequest> compositeRequests, long regionId, RegionEpoch regionEpoch) {
        super(regionId, regionEpoch);
        this.compositeRequests = compositeRequests;
    }

    public List<BaseRequest> getCompositeRequests() {
        return compositeRequests;
    }

    public void setCompositeRequests(List<BaseRequest> compositeRequests) {
        this.compositeRequests = compositeRequests;
    }

    @Override
    public byte magic() {
        return BATCH_COMPOSITE;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BatchCompositeRequest").append(" size=").append(compositeRequests.size()).append('{');
        for (BaseRequest request : compositeRequests) {
            sb.append(' ').append(request.toString());
        }
        sb.append("}");
        return sb.toString();
    }
}
