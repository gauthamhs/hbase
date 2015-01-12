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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureFairRunQueues;
import org.apache.hadoop.hbase.procedure2.ProcedureRunnableSet;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;

@InterfaceAudience.Private
public class MasterProcedureQueue implements ProcedureRunnableSet {
  private static final Log LOG = LogFactory.getLog(MasterProcedureQueue.class);

  private final ProcedureFairRunQueues<TableName, TableRunQueue> fairq;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();
  private final TableLockManager lockManager;

  private int queueSize;

  public MasterProcedureQueue(final TableLockManager lockManager) {
    this.fairq = new ProcedureFairRunQueues<TableName, TableRunQueue>(1);
    this.lockManager = lockManager;
  }

  @Override
  public void addFront(final Procedure proc) {
    lock.lock();
    try {
      getRunQueue(proc).addFront(proc);
      queueSize++;
      waitCond.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addBack(final Procedure proc) {
    lock.lock();
    try {
      getRunQueue(proc).addBack(proc);
      queueSize++;
      waitCond.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void yield(final Procedure proc) {
    addFront(proc);
  }

  @Override
  public Long poll() {
    lock.lock();
    try {
      if (queueSize == 0) {
        waitCond.await();
        if (queueSize == 0) {
          return null;
        }
      }

      TableRunQueue queue = fairq.poll();
      if (queue != null && queue.isAvailable()) {
        queueSize--;
        return queue.poll();
      }
    } catch (InterruptedException e) {
      return null;
    } finally {
      lock.unlock();
    }
    return null;
  }

  @Override
  public void signalAll() {
    lock.lock();
    try {
      waitCond.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return queueSize;
    } finally {
      lock.unlock();
    }
  }

  private TableRunQueue getRunQueue(final Procedure proc) {
    if (proc instanceof TableProcedureInterface) {
      return getRunQueue(((TableProcedureInterface)proc).getTableName());
    }
    return null;
  }

  private TableRunQueue getRunQueue(final TableName table) {
    final TableRunQueue queue = fairq.get(table);
    return queue != null ? queue : fairq.add(table, new TableRunQueue());
  }

  public boolean tryAcquireTableRead(final TableName table) {
    return getRunQueue(table).tryRead(lockManager, table, "");
  }

  public void releaseTableRead(final TableName table) {
    getRunQueue(table).releaseRead();
  }

  public boolean tryAcquireTableWrite(final TableName table) {
    return getRunQueue(table).tryWrite(lockManager, table, "");
  }

  public void releaseTableWrite(final TableName table) {
    getRunQueue(table).releaseWrite();
  }

  private static class TableRunQueue implements ProcedureFairRunQueues.FairObject {
    enum State { UNKNOWN, CREATING, AVAILABLE, DELETING }

    private final Deque<Long> runnables = new ArrayDeque<Long>();

    private State state = State.UNKNOWN;
    private TableLock tableLock = null;
    private boolean xlock = false;
    private int rlock = 0;

    public void addFront(final Procedure proc) {
      runnables.addFirst(proc.getProcId());
    }

    public void addBack(final Procedure proc) {
      runnables.addLast(proc.getProcId());
    }

    public Long poll() {
      return runnables.poll();
    }

    public boolean isAvailable() {
      synchronized (this) {
        return !xlock && !runnables.isEmpty();
      }
    }

    public boolean isEmpty() {
      return runnables.isEmpty();
    }

    public boolean tryRead(final TableLockManager lockManager,
        final TableName tableName, final String purpose) {
      synchronized (this) {
        if (xlock) {
          return false;
        }

        // Take zk-read-lock
        tableLock = lockManager.readLock(tableName, purpose);
        try {
          tableLock.acquire();
        } catch (IOException e) {
          MasterProcedureQueue.LOG.error("failed acquire read lock on " + tableName, e);
          tableLock = null;
          return false;
        }

        rlock++;
      }
      return true;
    }

    public void releaseRead() {
      synchronized (this) {
        releaseTableLock();
        rlock--;
      }
    }

    public boolean tryWrite(final TableLockManager lockManager,
        final TableName tableName, final String purpose) {
      synchronized (this) {
        if (xlock) {
          return false;
        }

        // Take zk-write-lock
        MasterProcedureQueue.LOG.info("Try write lock " + tableName);
        tableLock = lockManager.writeLock(tableName, purpose);
        try {
          tableLock.acquire();
        } catch (IOException e) {
          MasterProcedureQueue.LOG.error("failed acquire write lock on " + tableName, e);
          tableLock = null;
          return false;
        }
        xlock = true;
      }
      return true;
    }

    public void releaseWrite() {
      synchronized (this) {
        releaseTableLock();
        xlock = false;
      }
    }

    private void releaseTableLock() {
      for (int i = 0; i < 3; ++i) {
        try {
          tableLock.release();
          tableLock = null;
          break;
        } catch (IOException e) {
          LOG.warn("Could not release the table write-lock", e);
        }
      }
    }

    @Override
    public int getPriority() {
      return 1;
    }
  }
}
