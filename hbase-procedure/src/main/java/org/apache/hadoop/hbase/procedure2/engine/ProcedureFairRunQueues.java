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

package org.apache.hadoop.hbase.procedure2.engine;

import java.util.Map;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

public class ProcedureFairRunQueues<TKey> {
  private ConcurrentSkipListMap<TKey, FairQueue> objMap =
    new ConcurrentSkipListMap<TKey, FairQueue>();

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();
  private final int quantum;

  private Map.Entry<TKey, FairQueue> current;
  private int currentQuantum = 0;
  private int prioSum = 0;

  private static class FairQueue {
    private final ProcedureRunnableSet queue;
    private final int prio;

    public FairQueue(int prio, ProcedureRunnableSet queue) {
      this.queue = queue;
      this.prio = prio;
    }

    public int getPriority() {
      return prio;
    }

    public ProcedureRunnableSet getQueue() {
      return queue;
    }
  }

  public ProcedureFairRunQueues(int quantum) {
    this.quantum = quantum;
  }

  public ProcedureRunnableSet get(TKey key) {
    FairQueue obj = objMap.get(key);
    return obj != null ? obj.getQueue() : null;
  }

  public ProcedureRunnableSet add(TKey key, int prio) {
    ProcedureRunnableSet queue = new ProcedureSingleRunQueue();
    add(key, prio, queue);
    return queue;
  }

  public void add(TKey key, int prio, ProcedureRunnableSet queue) {
    objMap.put(key, new FairQueue(prio, queue));
  }

  public void remove(TKey key) {
    objMap.remove(key);
  }

  public ProcedureRunnableSet poll() {
    lock.lock();
    try {
      if (currentQuantum == 0) {
        // If we have already a key, try the next one
        if (current != null) {
          current = objMap.higherEntry(current.getKey());
        }

        // if there is no higher key, go back to the first
        if (current == null) {
          current = objMap.firstEntry();
        }

        FairQueue object = current.getValue();
        currentQuantum = calculateQuantum(object) - 1;
        return object.getQueue();
      } else {
        currentQuantum--;
        return current.getValue().getQueue();
      }
    } finally {
      lock.unlock();
    }
  }

  private int calculateQuantum(final FairQueue fairObject) {
    // TODO
    return Math.max(1, Math.round(fairObject.getPriority() * quantum));
  }
}