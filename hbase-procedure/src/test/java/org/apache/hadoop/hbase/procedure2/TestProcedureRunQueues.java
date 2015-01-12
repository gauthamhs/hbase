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

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureRunQueues {
  class TestRunQueue implements ProcedureFairRunQueues.FairObject {
    private final String name;
    private final int priority;

    public TestRunQueue(String name, int priority) {
      this.name = name;
      this.priority = priority;
    }

    @Override
    public String toString() {
      return name;
    }

    @Override
    public boolean isAvailable() {
      return true;
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }

  @Test
  public void testFairQueues() throws Exception {
    ProcedureFairRunQueues<String, TestRunQueue> fairq
      = new ProcedureFairRunQueues<String, TestRunQueue>(1);
    TestRunQueue a = fairq.add("A", new TestRunQueue("A", 1));
    TestRunQueue b = fairq.add("B", new TestRunQueue("B", 1));
    TestRunQueue m = fairq.add("M", new TestRunQueue("M", 2));

    for (int i = 0; i < 3; ++i) {
      assertEquals(a, fairq.poll());
      assertEquals(b, fairq.poll());
      assertEquals(m, fairq.poll());
      assertEquals(m, fairq.poll());
    }
  }
}