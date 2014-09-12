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

package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureStoreTracker {
  static class TestProcedure extends Procedure {
    public TestProcedure(long procId) {
      setProcId(procId);
    }

    @Override
    public Procedure[] execute() { return null; }

    @Override
    public void rollback() { /* no-op */ }

    @Override
    public void abort() { /* no-op */ }

    @Override
    public void serializeStateData(final OutputStream stream) { /* no-op */ }

    @Override
    public void deserializeStateData(final InputStream stream) { /* no-op */ }
  }

  @Test
  public void testCRUD() {
    ProcedureStoreTracker tracker = new ProcedureStoreTracker();
    assertTrue(tracker.isEmpty());

    Procedure[] procs = new TestProcedure[] {
      new TestProcedure(1), new TestProcedure(2), new TestProcedure(3),
      new TestProcedure(4), new TestProcedure(5), new TestProcedure(6),
    };

    tracker.insert(procs[0], null);
    tracker.insert(procs[1], new Procedure[] { procs[2], procs[3], procs[4] });
    assertFalse(tracker.isEmpty());
    assertTrue(tracker.isUpdated());

    tracker.resetUpdates();
    assertFalse(tracker.isUpdated());

    for (int i = 0; i < 4; ++i) {
      tracker.update(procs[i]);
      assertFalse(tracker.isEmpty());
      assertFalse(tracker.isUpdated());
    }

    tracker.update(procs[4]);
    assertFalse(tracker.isEmpty());
    assertTrue(tracker.isUpdated());

    tracker.update(procs[5]);
    assertFalse(tracker.isEmpty());
    assertTrue(tracker.isUpdated());

    for (int i = 0; i < 5; ++i) {
      tracker.delete(procs[i].getProcId());
      assertFalse(tracker.isEmpty());
      assertTrue(tracker.isUpdated());
    }
    tracker.delete(procs[5].getProcId());
    assertTrue(tracker.isEmpty());
  }
}
