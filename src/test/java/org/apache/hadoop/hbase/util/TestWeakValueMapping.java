/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * These tests demonstrate the basic WeakValueMapping semantics.
 */
@Category(SmallTests.class)
public class TestWeakValueMapping {

  @Test
  public void testGetPutRemove() {
    WeakValueMapping<Object, Object> wvm = new WeakValueMapping<Object, Object>();
    Object k1 = new Object();
    Object v1 = new Object();
    Object k2 = new Object();
    Object v2 = new Object();

    // add, get, count values
    assertEquals(null, wvm.put(k1,  v1));
    assertEquals(v1, wvm.get(k1));
    assertEquals(1, wvm.liveSet().size());

    // add a second
    assertEquals(null, wvm.put(k2, v2));
    assertEquals(2, wvm.liveSet().size());

    // remove one and count
    assertEquals(v1, wvm.remove(k1));
    assertEquals(1, wvm.liveSet().size());
  }

  @Test
  public void testWeakRefExpire() throws InterruptedException {
    WeakValueMapping<Object, Object> wvm = new WeakValueMapping<Object, Object>();

    // Create a lot of kv pairs (enough to need a gc)
    int size = 100000;
    Object[] strongValueRefs = new Object[size];
    for (int i = 0; i < size; i++) {
      Object k = new Object();
      strongValueRefs[i] = new Object();
      wvm.put(k, strongValueRefs[i]);
    }
    assertEquals(size, wvm.liveSet().size());

    // null out the strong references to them.
    for (int i = 0; i < size; i++) {
      Object k = new Object();
      strongValueRefs[i] = null;
      wvm.put(k, strongValueRefs[i]);
    }

    // try to force gc to clean up all the kv pairs
    System.gc();

    // the live set is now empty!
    assertEquals(0, wvm.liveSet().size());
  }

}
