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
package com.alipay.sofa.jraft.rhea.storage;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.failover.impl.BoolFailoverFuture;
import com.alipay.sofa.jraft.rhea.errors.Errors;

public class CountDownKVStoreClosure implements KVStoreClosure {
    final private KVStoreClosure closureKeeper; // called only when all kvStoreClosures are completed
    private Status               mergedStatus; // any failure
    private int                  countDown;

    public CountDownKVStoreClosure(int count, KVStoreClosure closureKeeper) {
        this.closureKeeper = closureKeeper;
        this.mergedStatus = Status.OK();
        this.countDown = count;
    }

    @Override
    public Errors getError() {
        return null;
    }

    @Override
    public void setError(Errors error) {

    }

    @Override
    public Object getData() {
        return null;
    }

    @Override
    public void setData(Object data) {

    }

    @Override
    public void run(final Status status) {
        if (!status.equals(Status.OK())) {
            mergedStatus = status;
            // TODO : do early complete on any error
        }
        if (--this.countDown <= 0) {
            if (closureKeeper != null) {
                closureKeeper.setData(mergedStatus.equals(Status.OK()));
                closureKeeper.run(mergedStatus);
            }
        }
    }

}
