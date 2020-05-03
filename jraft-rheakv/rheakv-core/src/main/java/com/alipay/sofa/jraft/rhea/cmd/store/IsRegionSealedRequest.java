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

/**
 *
 * @author Jerry Yang
 */
public class IsRegionSealedRequest extends BaseRequest {

    private static final long serialVersionUID = 6601415966181665577L;

    public IsRegionSealedRequest() {
    }

    public IsRegionSealedRequest(long regionId, RegionEpoch regionEpoch) {
        super(regionId, regionEpoch);
    }

    @Override
    public byte magic() {
        return IS_REGION_SEALED;
    }

    @Override
    public String toString() {
        return "IsRegionSealedRequest{" + "regionId=" + getRegionId() + ", regionEpoch=" + getRegionEpoch() + "} ";
    }
}
