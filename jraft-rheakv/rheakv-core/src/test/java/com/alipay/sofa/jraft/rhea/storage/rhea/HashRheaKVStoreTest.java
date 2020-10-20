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
package com.alipay.sofa.jraft.rhea.storage.rhea;

import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.PMemDBOptions;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.StorageType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeKey;
import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeValue;
import static org.junit.Assert.*;

/**
 * HashRheaKVStoreTest unit test, covering the interfaces with regionId argument of {@link RheaKVStore}
 * when regionId is specified on interface, there may be overlap between regions.
 * But for a given region, the keys inside a region are ordered.
 * So below key mappings are valid, where so called Hash Region.
 *    Region-1 : A B C
 *    Region-2 : B C D
 *
 * @author Jerry Yang
 */
public abstract class HashRheaKVStoreTest extends RheaKVTestCluster {


}
