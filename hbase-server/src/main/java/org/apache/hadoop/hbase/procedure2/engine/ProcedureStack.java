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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureStack {
  private static final Log LOG = LogFactory.getLog(ProcedureStack.class);

  private enum State {
    RUNNING,
    FAILED,
    ROLLINGBACK,
  }

  private ArrayList<Procedure> subprocedures = null;
  private State state = State.RUNNING;
  private int running = 0;

  public synchronized boolean isFailed() {
    switch (state) {
      case ROLLINGBACK:
      case FAILED:
        return true;
    }
    return false;
  }

  public synchronized boolean isRollingback() {
    return state == State.ROLLINGBACK;
  }

  synchronized boolean setRollback() {
    if (running == 0 && state == State.FAILED) {
      state = State.ROLLINGBACK;
      return true;
    }
    return false;
  }

  List<Procedure> getSubprocedures() {
    return subprocedures;
  }

  synchronized boolean acquire(final Procedure proc) {
    if (state != State.RUNNING) return false;

    running++;
    return true;
  }

  synchronized void release(final Procedure proc) {
    running--;
  }

  synchronized void addRollbackStep(final Procedure proc) {
    if (proc.isFailed()) {
      state = State.FAILED;
    }
    if (subprocedures == null) {
      subprocedures = new ArrayList<Procedure>();
    }
    proc.addStackIndex(subprocedures.size());
    subprocedures.add(proc);
  }

  void loadStack(final Procedure proc) {
    int[] stackIndexes = proc.getStackIndexes();
    if (stackIndexes != null) {
      if (subprocedures == null) {
        subprocedures = new ArrayList<Procedure>();
      }
      int diff = (1 + stackIndexes[stackIndexes.length - 1]) - subprocedures.size();
      if (diff > 0) {
        subprocedures.ensureCapacity(1 + stackIndexes[stackIndexes.length - 1]);
        while (diff-- > 0) subprocedures.add(null);
      }
      for (int i = 0; i < stackIndexes.length; ++i) {
        subprocedures.set(stackIndexes[i], proc);
      }
    }
    if (proc.getState() == ProcedureState.ROLLEDBACK) {
      state = State.ROLLINGBACK;
    } else if (proc.isFailed()) {
      state = State.FAILED;
    }
  }

  boolean isValid() {
    if (subprocedures != null) {
      for (Procedure proc: subprocedures) {
        if (proc == null) {
          return false;
        }
      }
    }
    return true;
  }
}