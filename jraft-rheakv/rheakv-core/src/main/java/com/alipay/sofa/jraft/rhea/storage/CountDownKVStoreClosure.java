package com.alipay.sofa.jraft.rhea.storage;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.failover.impl.BoolFailoverFuture;
import com.alipay.sofa.jraft.rhea.errors.Errors;

public class CountDownKVStoreClosure implements KVStoreClosure {
    final private KVStoreClosure closureKeeper; // called only when all kvStoreClosures are completed
    private Status mergedStatus; // any failure
    private int countDown;

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
