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

package org.apache.hadoop.hbase.master.procedure;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.engine.ProcedureRunnableSet;

/**
 * Keep track of the runnable procedures
 */
@InterfaceAudience.Private
public class MasterProcedureQueue {
/*
  private FairRunSet<TableName, ProcedureQueue> tableQueues;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();

  private class ProcedureQueue : ArrayDeque<Long> {
    public void add(Procedure proc) {
      add(proc.getProcId());
    }
  }

  public MasterProcedureQueue() {
  }

  public void add(Procedure proc) {
    if (proc instanceof TableProcedureInterface) {
      TableProcedureInterface tableProc = (TableProcedureInterface)proc;
      ProcedureQueue queue = tablesQueue.get(tableProc.getTableName());
      if (queue == null) {
        ProcedureQueue newQueue = new ProcedureQueue();
        int prio = getTablePriority(tableProc.getTableName());
        queue = tablesQueue.putIfAbsent(tableProc.getTableName(), newQueue, prio);
        if (queue == null) queue = newQueue;
      }
      addToQueue(defaultQueue, proc);
    } else {
      throw new Exception("Not Implemented Yet");
    }
  }

  private int getTablePriority(final TableName table) {
    if (table.isSystemTable()) {
      return 2;
    }
    return 1;
  }

  private void addToQueue(ProcedureQueue queue, Procedure proc) {
    lock.lock();
    try {
      queue.add(proc);
      waitCond.signal();
    } finally {
      lock.unlock();
    }
  }

  public Long poll() {
    lock.lock();
    try {

      if (runnables.isEmpty()) {
        waitCond.await();
        if (!runnables.isEmpty()) {
          return runnables.pop();
        }
      } else {
        return runnables.pop();
      }
    } catch (InterruptedException e) {
      return null;
    } finally {
      lock.unlock();
    }
    return null;
  }

  public void signalAll() {
    lock.lock();
    try {
      waitCond.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public int size() {
    return 0;
  }

  // ==========================================================================
  //
  // ==========================================================================
  public class FairRunSet<TKey, TValue> {
    private ConcurrentSkipListMap<TKey, FairObject> objMap =
      new ConcurrentSkipListMap<TKey, FairObject>();

    private Map.Entry<TKey, FairObject> current;
    private int currentQuantum = 0;
    private int prioSum = 0;

    private class FairObject {
      private TValue object;
      private float prio;

      public FairObject(final TValue object, int prio) {
        this.object = object;
        this.prio = prio;
      }

      public int getPriority() {
        return prio;
      }

      public TValue getObject() {
        return object;
      }
    }

    public FairRunQueue(int quantum) {
      this.quantum = quantum;
    }

    public TValue get(TKey key) {
      FairObject obj = objMap.get(key);
      return obj != null ? obj.getObject() : null;
    }

    public TValue putIfAbsent(TKey key, TValue object, int prio) {
      FairObject oldObj = objMap.putIfAbsent(key, new FairObject(object, prio));
      return oldObj != null ? oldObj.getObject() : object;
    }

    public void remove(TKey key) {
      objMap.remove(key);
    }

    public TValue poll() {
      if (currentQuantum == 0) {
        // If we have already a key, try the next one
        if (current != null) {
          current = objMap.higherEntry(current.getKey());
        }

        // if there is no higher key, go back to the first
        if (current == null) {
          current = objMap.firstEntry();
        }

        FairObject object = current.getValue();
        currentQuantum = calculateQuantum(object) - 1;
        return object.getObject();
      } else {
        currentQuantum--;
        return current.getValue().getObject();
      }
    }

    private int calculateQuantum(final FairObject fairObject) {
      return Math.max(1, Math.round(fairObject.getPriority() * quantum));
    }
  }
*/
}