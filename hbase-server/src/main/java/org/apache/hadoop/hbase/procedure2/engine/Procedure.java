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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Base Procedure class responsible to handle the Procedure Metadata
 * e.g. state, startTime, lastUpdate, stack-indexes, ...
 *
 * execute() is called each time the procedure is executed.
 * it may be called multiple time in case of failure and restart, so the
 * code must be idempotent.
 * the return is a set of sub-procedures or null in case the procedure doesn't
 * have sub-procedures. Once the sub-procedures are successfully completed
 * the execute() method is called again.
 *
 * rollback() is called when the procedure or one of the sub-procedures is failed.
 * the rollback step is supposed to cleanup the resources created during the
 * execute() step.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Procedure {
  private ProcedureState state = ProcedureState.INITIALIZING;
  private ForeignException exception = null;
  private int[] stackIndexes = null;
  private int childrenLatch = 0;
  private byte[] result = null;

  private Integer timeout = null;
  private Long parentProcId = null;
  private Long procId = null;
  private long lastUpdate;
  private long startTime;
  private int type;

  public abstract Procedure[] execute();
  public abstract void rollback();
  public abstract void abort();

  public abstract void serializeStateData(final OutputStream stream)
    throws IOException;
  public abstract void deserializeStateData(final InputStream stream)
    throws IOException;

  public byte[] getResult() {
    return result;
  }

  public void setResult(final byte[] result) {
    this.result = result;
  }

  public int getType() {
    return 0;
  }

  @InterfaceAudience.Private
  protected Procedure[] doExecute() {
    updateTimestamp();
    return execute();
  }

  @InterfaceAudience.Private
  protected void doRollback() {
    rollback();
  }

  public ProcedureState getState() {
    return state;
  }

  public long getProcId() {
    return procId;
  }

  public boolean hasParent() {
    return parentProcId != null;
  }

  public boolean hasException() {
    return exception != null;
  }

  public boolean hasTimeout() {
    return timeout != null;
  }

  public long getParentProcId() {
    return parentProcId;
  }

  public boolean isFailed() {
    return exception != null || state == ProcedureState.ROLLEDBACK;
  }

  public boolean isCompleted() {
    return state == ProcedureState.FINISHED && exception == null;
  }

  public boolean isFinished() {
    switch (state) {
      case ROLLEDBACK:
      case FINISHED:
        return true;
    }
    return false;
  }

  public ForeignException getException() {
    return exception;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public int getTimeout() {
    return timeout;
  }

  public long getMillisToTimeout() {
    return Math.max(0, timeout - (EnvironmentEdgeManager.currentTime() - lastUpdate));
  }

  protected void updateTimestamp() {
    this.lastUpdate = EnvironmentEdgeManager.currentTime();
  }

  public synchronized void setState(final ProcedureState state) {
    this.state = state;
    updateTimestamp();
  }

  public synchronized void setFailure(final ForeignException exception) {
    this.exception = exception;
    setState(ProcedureState.FINISHED);
  }

  @InterfaceAudience.Private
  synchronized boolean setTimeoutFailure() {
    if (state == ProcedureState.WAITING_TIMEOUT) {
      long timeDiff = EnvironmentEdgeManager.currentTime() - lastUpdate;
      setFailure(new ForeignException("ProcedureExecutor", new TimeoutException(
        "Operation timed out after " + StringUtils.formatTime(timeDiff))));
      return true;
    }
    return false;
  }


  @InterfaceAudience.Private
  public void setProcId(final long procId) {
    this.procId = procId;
    this.state = ProcedureState.RUNNABLE;
    this.startTime = EnvironmentEdgeManager.currentTime();
    this.lastUpdate = startTime;
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  public void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  public void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  public void setLastUpdate(final long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  public void setParentProcId(final long parentProcId) {
    this.parentProcId = parentProcId;
  }

  /**
   * Called by the ProcedureExecutor on procedure-load to restore the latch state
   */
  @InterfaceAudience.Private
  synchronized void setChildrenLatch(final int numChildren) {
    this.childrenLatch = numChildren;
  }

  /**
   * Called by the ProcedureExecutor on procedure-load to restore the latch state
   */
  @InterfaceAudience.Private
  void incChildrenLatch() {
    // TODO: can this be inferred from the stack? Jag tror det...
    this.childrenLatch++;
  }

  /**
   * Called by the ProcedureExecutor to notify that one of the sub-procedures
   * has completed.
   */
  @InterfaceAudience.Private
  synchronized boolean childrenCountDown() {
    assert childrenLatch > 0;
    return --childrenLatch == 0;
  }

  /**
   * Called by the ProcedureStack on procedure execution.
   * Each procedure store its stack-index positions.
   */
  @InterfaceAudience.Private
  void addStackIndex(final int index) {
    if (stackIndexes == null) {
      stackIndexes = new int[] { index };
    } else {
      int[] newStackIndexes = new int[stackIndexes.length + 1];
      System.arraycopy(stackIndexes, 0, newStackIndexes, 0, stackIndexes.length);
      newStackIndexes[stackIndexes.length] = index;
      stackIndexes = newStackIndexes;
    }
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  public void setStackIndexes(final List<Integer> stackIndexes) {
    this.stackIndexes = new int[stackIndexes.size()];
    for (int i = 0; i < this.stackIndexes.length; ++i) {
      this.stackIndexes[i] = stackIndexes.get(i);
    }
  }

  @InterfaceAudience.Private
  boolean wasExecuted() {
    return stackIndexes != null;
  }

  @InterfaceAudience.Private
  public int[] getStackIndexes() {
    return stackIndexes;
  }

  /*
   * Helper to lookup the root Procedure ID given a specified procedure.
   */
  @InterfaceAudience.Private
  static Long getRootProcedureId(final Map<Long, Procedure> procedures, final Procedure proc) {
    while (proc.hasParent()) {
      proc = procedures.get(proc.getParentProcId());
      if (proc == null) return null;
    }
    return proc.getProcId();
  }
}