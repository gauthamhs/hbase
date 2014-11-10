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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

// TODO: Add runqueue per type
/**
 * Simple FIFO runqueue for the procedures
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureSingleRunQueue implements ProcedureRunnableSet {
  private final Log LOG = LogFactory.getLog(ProcedureSingleRunQueue.class);

  private final Deque<Long> runnables = new ArrayDeque<Long>();
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();

  @Override
  public void add(final Procedure proc) {
    lock.lock();
    try {
      runnables.add(proc.getProcId());
      waitCond.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
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
      return runnables.size();
    } finally {
      lock.unlock();
    }
  }
}