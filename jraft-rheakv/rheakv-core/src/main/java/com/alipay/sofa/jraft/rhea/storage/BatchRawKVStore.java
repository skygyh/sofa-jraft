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
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.StorageException;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;

import java.util.ArrayList;
import java.util.List;

/**
 * The default batch write implementation, without any optimization,
 * subclasses need to override and optimize.
 *
 * @author jiachun.fjc
 */
public abstract class BatchRawKVStore<T> extends BaseRawKVStore<T> {

    protected BatchRawKVStore() {
        super();
    }

    protected BatchRawKVStore(final long regionId, final String dbPath) {
        super(regionId, dbPath);
    }

    public void batchPut(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            put(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchPutIfAbsent(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            putIfAbsent(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchPutList(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            put(kvState.getOp().getEntries(), kvState.getDone());
        }
    }

    public void batchDelete(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            delete(kvState.getOp().getKey(), kvState.getDone());
        }
    }

    public void batchDeleteRange(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            deleteRange(op.getStartKey(), op.getEndKey(), kvState.getDone());
        }
    }

    public void batchDeleteList(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            delete(kvState.getOp().getKeys(), kvState.getDone());
        }
    }

    public void batchGetSequence(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            getSequence(op.getSeqKey(), op.getStep(), kvState.getDone());
        }
    }

    public void batchNodeExecute(final KVStateOutputList kvStates, final boolean isLeader) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            execute(kvState.getOp().getNodeExecutor(), isLeader, kvState.getDone());
        }
    }

    public void batchTryLockWith(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            final Pair<Boolean, DistributedLock.Acquirer> acquirerPair = op.getAcquirerPair();
            tryLockWith(op.getKey(), op.getFencingKey(), acquirerPair.getKey(), acquirerPair.getValue(),
                kvState.getDone());
        }
    }

    public void batchReleaseLockWith(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            releaseLockWith(op.getKey(), op.getAcquirer(), kvState.getDone());
        }
    }

    public void batchGet(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            get(kvState.getOp().getKey(), kvState.getDone());
        }
    }

    public void batchMultiGet(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            multiGet(kvState.getOp().getKeyList(), kvState.getDone());
        }
    }

    public void batchContainsKey(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            containsKey(kvState.getOp().getKey(), kvState.getDone());
        }
    }

    public void batchScan(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            scan(op.getStartKey(), op.getEndKey(), op.getLimit(), true, op.isReturnValue(), kvState.getDone());
        }
    }

    public void batchGetAndPut(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            getAndPut(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchCompareAndPut(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            compareAndPut(op.getKey(), op.getExpect(), op.getValue(), kvState.getDone());
        }
    }

    public void batchMerge(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            final KVOperation op = kvState.getOp();
            merge(op.getKey(), op.getValue(), kvState.getDone());
        }
    }

    public void batchResetSequence(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            resetSequence(kvState.getOp().getKey(), kvState.getDone());
        }
    }

    public void batchComposite(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            batch(kvState.getOp().getKVOperations(), kvState.getDone());
        }
    }

    public void batchDestroy(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            destroy(kvState.getOp().getRegionId(), kvState.getDone());
        }
    }

    public void batchSeal(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            seal(kvState.getOp().getRegionId(), kvState.getDone());
        }
    }

    public void batchQuerySealed(final KVStateOutputList kvStates) {
        for (int i = 0, l = kvStates.size(); i < l; i++) {
            final KVState kvState = kvStates.get(i);
            isSealed(kvState.getOp().getRegionId(), kvState.getDone());
        }
    }

    // called by batch in subclass RawKVStore
    protected void doBatch(final List<KVOperation> kvOperations, final KVStoreClosure closure) throws StorageException {
        final List<KVStoreClosure> subClosures = new ArrayList<>(kvOperations.size());
        final CountDownKVStoreClosure countDownClosure = new CountDownKVStoreClosure(kvOperations.size(), closure);
        for (int i = 0; i < kvOperations.size(); i++) {
            subClosures.add(new BaseKVStoreClosure() {
                @Override
                public void run(Status status) {
                    countDownClosure.run(status);
                }
            });
        }
        try {
            for (int i = 0; i < kvOperations.size(); i++) {
                doSingleOperation(kvOperations.get(i), subClosures.get(i));
            }
            setSuccess(closure, Boolean.TRUE);
        } catch (final Exception e) {
            setCriticalError(closure, "Fail to [BATCH_OP]", e);
        }
    }

    // called by batch only, recursively for COMPOSITE_OP
    protected void doSingleOperation(KVOperation op, final KVStoreClosure closure) {
        switch (op.getOp()) {
            case KVOperation.PUT:
                put(op.getKey(), op.getValue(), closure);
                break;
            case KVOperation.PUT_IF_ABSENT:
                putIfAbsent(op.getKey(), op.getValue(), closure);
                break;
            case KVOperation.DELETE:
                delete(op.getKey(), closure);
                break;
            case KVOperation.PUT_LIST:
                KVStateOutputList kvStates = KVStateOutputList.newInstance();
                kvStates.add(KVState.of(op, closure));
                batchPut(kvStates);
                break;
            case KVOperation.DELETE_RANGE:
                deleteRange(op.getStartKey(), op.getEndKey(), closure);
                break;
            case KVOperation.GET_SEQUENCE:
                getSequence(op.getSeqKey(), op.getStep(), closure);
                break;
            case KVOperation.NODE_EXECUTE:
                closure.setError(Errors.INVALID_PARAMETER);
                throw new UnsupportedOperationException("Request " + op.getOp() + " is not supported");
            case KVOperation.KEY_LOCK:
                tryLockWith(op.getKey(), op.getValue(), true, op.getAcquirer(), closure);
                break;
            case KVOperation.KEY_LOCK_RELEASE:
                releaseLockWith(op.getKey(), op.getAcquirer(), closure);
                break;
            case KVOperation.GET:
                get(op.getKey(), closure);
                break;
            case KVOperation.MULTI_GET:
                multiGet(op.getKeys(), closure);
                break;
            case KVOperation.SCAN:
                scan(op.getStartKey(), op.getEndKey(), closure);
                break;
            case KVOperation.GET_PUT:
                getAndPut(op.getKey(), op.getValue(), closure);
                break;
            case KVOperation.MERGE:
                merge(op.getKey(), op.getValue(), closure);
                break;
            case KVOperation.RESET_SEQUENCE:
                resetSequence(op.getSeqKey(), closure);
                break;
            case KVOperation.RANGE_SPLIT:
                break;
            case KVOperation.COMPARE_PUT:
                compareAndPut(op.getKey(), op.getExpect(), op.getValue(), closure);
                break;
            case KVOperation.DELETE_LIST:
                KVStateOutputList kvStates2 = KVStateOutputList.newInstance();
                kvStates2.add(KVState.of(op, closure));
                batchDelete(kvStates2);
                break;
            case KVOperation.CONTAINS_KEY:
                containsKey(op.getKey(), closure);
                break;
            case KVOperation.BATCH_OP:
                // seriously
                doSingleOperation(op, closure);
                break;
            case KVOperation.DESTROY:
                destroy(op.getRegionId(), closure);
                break;
            case KVOperation.SEAL:
                seal(op.getRegionId(), closure);
                break;
            default:
                closure.setError(Errors.INVALID_PARAMETER);
                throw new UnsupportedOperationException("batch op " + op.getOp() + " is not supported yet");
        }
    }
}
