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

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.RheaKVServiceFactory;
import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVCliService;
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
import org.junit.*;
import org.junit.rules.TestName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeKey;
import static com.alipay.sofa.jraft.rhea.KeyValueTool.makeValue;
import static org.junit.Assert.*;

/**
 * RheaKVStore unit test, covering all interfaces of {@link RheaKVStore}
 *
 * @author jiachun.fjc
 */
public abstract class AbstractRheaKVStoreTest extends RheaKVTestCluster {}
