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
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.procedure2.ProcedureAbortedException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
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
  private RemoteProcedureException exception = null;
  private int[] stackIndexes = null;
  private int childrenLatch = 0;
  private byte[] result = null;

  private Integer timeout = null;
  private Long parentProcId = null;
  private Long procId = null;
  private long lastUpdate;
  private long startTime;
  private int type;

  /**
   * The main code of the procedure. It must be idempotent since execute()
   * may be called multiple time in case of machine failure in the middle
   * of the execution.
   * @return a set of sub-procedures or null if there is nothing else to execute.
   */
  public abstract Procedure[] execute();

  /**
   * The reverse of the execute() code. It is called when the procedure or
   * one of the sub-procedure failed or an abort was requested.
   * It should cleanup all the resources created by the execute() call.
   */
  public abstract void rollback();

  /**
   * The abort() call is asynchronous and each procedure must decide how to deal
   * with that, if they want to be abortable. The simplest implementation
   * is to have an AtomicBoolean set in the abort() method and then the execute()
   * will check if the abort flag is set or not.
   *
   * The abort is a user-level responsability mainly because the user-level
   * code of the procedure should know what to rollback.
   */
  public abstract void abort();

  /**
   * The user-level code of the procedure may have some state to
   * persist (e.g. input arguments) to be able to resume on failure.
   * @param stream the stream that will contain the user serialized data
   */
  public abstract void serializeStateData(final OutputStream stream)
    throws IOException;

  /**
   * Called on store load to allow the user to decode the previously serialized
   * state.
   * @param stream the stream that contains the user serialized data
   */
  public abstract void deserializeStateData(final InputStream stream)
    throws IOException;

  /**
   * @return the serialized result if any, otherwise null
   */
  public byte[] getResult() {
    return result;
  }

  /**
   * The procedure may leave a "result" on completion.
   * @param result the serialized result that will be passed to the client
   */
  public void setResult(final byte[] result) {
    this.result = result;
  }

  /**
   * The user may decide to group the Procedures by Type by overriding this getType().
   * This is useful if multiple run-queues are used for the execution,
   * where each type may have a different "priority".
   */
  public int getType() {
    return 0;
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

  public boolean isWaiting() {
    switch (state) {
      case WAITING:
      case WAITING_TIMEOUT:
        return true;
    }
    return false;
  }

  public RemoteProcedureException getException() {
    return exception;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  public int getTimeout() {
    return timeout;
  }

  public long getMillisToTimeout() {
    return Math.max(0, timeout - (EnvironmentEdgeManager.currentTime() - lastUpdate));
  }

  public synchronized void setState(final ProcedureState state) {
    this.state = state;
    updateTimestamp();
  }

  @InterfaceAudience.Private
  public ProcedureState getState() {
    return state;
  }

  public synchronized void setFailure(final String source, final Throwable cause) {
    setFailure(new RemoteProcedureException(source, cause));
  }

  public synchronized void setFailure(final RemoteProcedureException exception) {
    this.exception = exception;
    if (!isFinished()) {
      setState(ProcedureState.FINISHED);
    }
  }

  protected synchronized void setAbortFailure() {
    setAbortFailure("user", "the user aborted the procedure");
  }

  protected synchronized void setAbortFailure(final String source, final String msg) {
    setFailure(source, new ProcedureAbortedException(msg));
  }

  @InterfaceAudience.Private
  synchronized boolean setTimeoutFailure() {
    if (state == ProcedureState.WAITING_TIMEOUT) {
      long timeDiff = EnvironmentEdgeManager.currentTime() - lastUpdate;
      setFailure("ProcedureExecutor", new TimeoutException(
        "Operation timed out after " + StringUtils.humanTimeDiff(timeDiff)));
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
   * Internal method called by the ProcedureExecutor that starts the
   * user-level code execute().
   */
  @InterfaceAudience.Private
  protected Procedure[] doExecute() {
    updateTimestamp();
    return execute();
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the
   * user-level code rollback().
   */
  @InterfaceAudience.Private
  protected void doRollback() {
    rollback();
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

  protected void updateTimestamp() {
    this.lastUpdate = EnvironmentEdgeManager.currentTime();
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
      int count = stackIndexes.length;
      stackIndexes = Arrays.copyOf(stackIndexes, count + 1);
      stackIndexes[count] = index;
    }
  }

  @InterfaceAudience.Private
  boolean removeStackIndex() {
    System.out.println("BE " + stackIndexes.length);
    if (stackIndexes.length > 0) {
      stackIndexes = Arrays.copyOf(stackIndexes, stackIndexes.length - 1);

      String x = "";
      for (int i = 0; i < stackIndexes.length; ++i)
        x += stackIndexes[i] + " ";

      System.out.println("BE " + stackIndexes.length + " " + x);
      return false;
    } else {
      stackIndexes = null;
      return true;
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
  static Long getRootProcedureId(final Map<Long, Procedure> procedures, Procedure proc) {
    while (proc.hasParent()) {
      proc = procedures.get(proc.getParentProcId());
      if (proc == null) return null;
    }
    return proc.getProcId();
  }

  public static Procedure newInstance(final String className) throws IOException {
    try {
      Class<?> clazz = Class.forName(className);
      Constructor<?> ctor = clazz.getConstructor();
      return (Procedure)ctor.newInstance();
    } catch (Exception e) {
      throw new IOException("The procedure class " + className +
          " must be accessible and have an empty constructor", e);
    }
  }

  public static void validateClass(final Procedure proc) throws IOException {
    try {
      Constructor<?> ctor = proc.getClass().getConstructor();
      assert ctor != null;
    } catch (Exception e) {
      throw new IOException("The procedure class " + proc.getClass().getName() +
          " must be accessible and have an empty constructor", e);
    }
  }
}