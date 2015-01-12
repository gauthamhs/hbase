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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.procedure2.ProcedureAbortedException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.util.ByteStringer;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

/**
 * Base Procedure class responsible to handle the Procedure Metadata
 * e.g. state, startTime, lastUpdate, stack-indexes, ...
 *
 * execute() is called each time the procedure is executed.
 * it may be called multiple time in case of failure and restart, so the
 * code must be idempotent.
 * the return is a set of sub-procedures or null in case the procedure doesn't
 * have sub-procedures. Once the sub-procedures are successfully completed
 * the execute() method is called again, you should think at it as a stack:
 *  -> step 1
 *  ---> step 2
 *  -> step 1
 *
 * rollback() is called when the procedure or one of the sub-procedures is failed.
 * the rollback step is supposed to cleanup the resources created during the
 * execute() step.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class Procedure<TEnvironment> {
  private ProcedureState state = ProcedureState.INITIALIZING;
  private RemoteProcedureException exception = null;
  private int[] stackIndexes = null;
  private int childrenLatch = 0;
  private byte[] result = null;

  private String owner = null;
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
  protected abstract Procedure[] execute(TEnvironment env);

  /**
   * The code to undo what done by the execute() code.
   * It is called when the procedure or one of the sub-procedure failed or an
   * abort was requested. It should cleanup all the resources created by
   * the execute() call.
   */
  protected abstract void rollback(TEnvironment env);

  /**
   * The abort() call is asynchronous and each procedure must decide how to deal
   * with that, if they want to be abortable. The simplest implementation
   * is to have an AtomicBoolean set in the abort() method and then the execute()
   * will check if the abort flag is set or not.
   *
   * NOTE: abort() is not like Thread.interrupt() it is just a notification
   * that allows the procedure implementor where to abort to avoid leak and
   * have a better control on what was executed and what not.
   */
  protected abstract void abort(TEnvironment env);

  /**
   * The user-level code of the procedure may have some state to
   * persist (e.g. input arguments) to be able to resume on failure.
   * @param stream the stream that will contain the user serialized data
   */
  protected abstract void serializeStateData(final OutputStream stream)
    throws IOException;

  /**
   * Called on store load to allow the user to decode the previously serialized
   * state.
   * @param stream the stream that contains the user serialized data
   */
  protected abstract void deserializeStateData(final InputStream stream)
    throws IOException;

  /**
   * The user should override this method, and try to take a lock if necessary.
   * A lock can be anything, and it is up to the implementor.
   * Example: in our Master we can execute request in parallel for different tables
   *          create t1 and create t2 can be executed at the same time.
   *          anything else on t1/t2 is queued waiting that specific table create to happen.
   *
   * @return true if the lock was acquired and false otherwise
   */
  protected boolean acquireLock(final TEnvironment env) {
    return true;
  }

  /**
   * The user should override this method, and release lock if necessary.
   */
  protected void releaseLock(final TEnvironment env) {
    // no-op
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName());

    sb.append(" id=");
    sb.append(getProcId());
    if (hasParent()) {
      sb.append(" parent=");
      sb.append(getParentProcId());
    }

    if (hasOwner()) {
      sb.append(" owner=");
      sb.append(getOwner());
    }

    sb.append(" state=");
    sb.append(getState());
    return sb.toString();
  }

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
  protected void setResult(final byte[] result) {
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

  /**
   * @return true if the procedure has failed.
   *         true may mean failed but not yet rolledback or failed and rolledback.
   */
  public boolean isFailed() {
    return exception != null || state == ProcedureState.ROLLEDBACK;
  }

  /**
   * @return true if the procedure is finished successfully.
   */
  public boolean isSuccess() {
    return state == ProcedureState.FINISHED && exception == null;
  }

  /**
   * @return true if the procedure is finished. The Procedure may be completed
   *         successfuly or failed and rolledback.
   */
  public boolean isFinished() {
    switch (state) {
      case ROLLEDBACK:
      case FINISHED:
        return true;
    }
    return false;
  }

  /**
   * @return true if the procedure is waiting for a child to finish or for an external event.
   */
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

  /**
   * @param timeout timeout in msec
   */
  protected void setTimeout(final int timeout) {
    this.timeout = timeout;
  }

  /**
   * @return the timeout in msec
   */
  public int getTimeout() {
    return timeout;
  }

  public long getMillisToTimeout() {
    return Math.max(0, timeout - (EnvironmentEdgeManager.currentTime() - lastUpdate));
  }

  protected void setOwner(final String owner) {
    this.owner = StringUtils.isEmpty(owner) ? null : owner;
  }

  public String getOwner() {
    return owner;
  }

  public boolean hasOwner() {
    return owner != null;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  protected synchronized void setState(final ProcedureState state) {
    this.state = state;
    updateTimestamp();
  }

  @InterfaceAudience.Private
  protected ProcedureState getState() {
    return state;
  }

  protected synchronized void setFailure(final String source, final Throwable cause) {
    setFailure(new RemoteProcedureException(source, cause));
  }

  protected synchronized void setFailure(final RemoteProcedureException exception) {
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
  protected synchronized boolean setTimeoutFailure() {
    if (state == ProcedureState.WAITING_TIMEOUT) {
      long timeDiff = EnvironmentEdgeManager.currentTime() - lastUpdate;
      setFailure("ProcedureExecutor", new TimeoutException(
        "Operation timed out after " + StringUtils.humanTimeDiff(timeDiff)));
      return true;
    }
    return false;
  }

  /**
   * Called by the ProcedureExecutor to assign the ID to the newly created procedure.
   */
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void setProcId(final long procId) {
    this.procId = procId;
    this.state = ProcedureState.RUNNABLE;
    this.startTime = EnvironmentEdgeManager.currentTime();
    this.lastUpdate = startTime;
  }

  /**
   * Called by the ProcedureExecutor to assign the parent to the newly created procedure.
   */
  @InterfaceAudience.Private
  protected void setParentProcId(final long parentProcId) {
    this.parentProcId = parentProcId;
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the
   * user-level code execute().
   */
  @InterfaceAudience.Private
  protected Procedure[] doExecute(final TEnvironment env) {
    updateTimestamp();
    return execute(env);
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the
   * user-level code rollback().
   */
  @InterfaceAudience.Private
  protected void doRollback(final TEnvironment env) {
    rollback(env);
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  private void setStartTime(final long startTime) {
    this.startTime = startTime;
  }

  /**
   * Called on store load to initialize the Procedure internals after
   * the creation/deserialization.
   */
  @InterfaceAudience.Private
  private void setLastUpdate(final long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  /**
   * Called by the ProcedureExecutor on procedure-load to restore the latch state
   */
  @InterfaceAudience.Private
  protected synchronized void setChildrenLatch(final int numChildren) {
    this.childrenLatch = numChildren;
  }

  protected void updateTimestamp() {
    this.lastUpdate = EnvironmentEdgeManager.currentTime();
  }

  /**
   * Called by the ProcedureExecutor on procedure-load to restore the latch state
   */
  @InterfaceAudience.Private
  protected void incChildrenLatch() {
    // TODO: can this be inferred from the stack? Jag tror det...
    this.childrenLatch++;
  }

  /**
   * Called by the ProcedureExecutor to notify that one of the sub-procedures
   * has completed.
   */
  @InterfaceAudience.Private
  protected synchronized boolean childrenCountDown() {
    assert childrenLatch > 0;
    return --childrenLatch == 0;
  }

  /**
   * Called by the ProcedureStack on procedure execution.
   * Each procedure store its stack-index positions.
   */
  @InterfaceAudience.Private
  protected void addStackIndex(final int index) {
    if (stackIndexes == null) {
      stackIndexes = new int[] { index };
    } else {
      int count = stackIndexes.length;
      stackIndexes = Arrays.copyOf(stackIndexes, count + 1);
      stackIndexes[count] = index;
    }
  }

  @InterfaceAudience.Private
  protected boolean removeStackIndex() {
    if (stackIndexes.length > 0) {
      stackIndexes = Arrays.copyOf(stackIndexes, stackIndexes.length - 1);
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
  protected void setStackIndexes(final List<Integer> stackIndexes) {
    this.stackIndexes = new int[stackIndexes.size()];
    for (int i = 0; i < this.stackIndexes.length; ++i) {
      this.stackIndexes[i] = stackIndexes.get(i);
    }
  }

  @InterfaceAudience.Private
  protected boolean wasExecuted() {
    return stackIndexes != null;
  }

  @InterfaceAudience.Private
  protected int[] getStackIndexes() {
    return stackIndexes;
  }

  /*
   * Helper to lookup the root Procedure ID given a specified procedure.
   */
  @InterfaceAudience.Private
  protected static Long getRootProcedureId(final Map<Long, Procedure> procedures, Procedure proc) {
    while (proc.hasParent()) {
      proc = procedures.get(proc.getParentProcId());
      if (proc == null) return null;
    }
    return proc.getProcId();
  }

  protected static Procedure newInstance(final String className) throws IOException {
    try {
      Class<?> clazz = Class.forName(className);
      Constructor<?> ctor = clazz.getConstructor();
      return (Procedure)ctor.newInstance();
    } catch (Exception e) {
      throw new IOException("The procedure class " + className +
          " must be accessible and have an empty constructor", e);
    }
  }

  protected static void validateClass(final Procedure proc) throws IOException {
    try {
      Constructor<?> ctor = proc.getClass().getConstructor();
      assert ctor != null;
    } catch (Exception e) {
      throw new IOException("The procedure class " + proc.getClass().getName() +
          " must be accessible and have an empty constructor", e);
    }
  }

  /**
   * Serialize the given procedure to the specified output stream.
   * Used by ProcedureStore implementations.
   */
  @InterfaceAudience.Private
  public static void serializeProcedure(final OutputStream out, final Procedure proc)
      throws IOException {
    assert proc != null;
    ProcedureProtos.Procedure.Builder builder = ProcedureProtos.Procedure.newBuilder()
      .setClassName(proc.getClass().getName())
      .setProcId(proc.getProcId())
      .setState(proc.getState())
      .setStartTime(proc.getStartTime())
      .setLastUpdate(proc.getLastUpdate());

    if (proc.hasParent()) {
      builder.setParentId(proc.getParentProcId());
    }

    if (proc.hasTimeout()) {
      builder.setTimeout(proc.getTimeout());
    }

    if (proc.hasOwner()) {
      builder.setOwner(proc.getOwner());
    }

    int[] stackIds = proc.getStackIndexes();
    if (stackIds != null) {
      for (int i = 0; i < stackIds.length; ++i) {
        builder.addStackId(stackIds[i]);
      }
    }

    if (proc.hasException()) {
      RemoteProcedureException exception = proc.getException();
      builder.setException(
        RemoteProcedureException.toProto(exception.getSource(), exception.getCause()));
    }

    byte[] result = proc.getResult();
    if (result != null) {
      builder.setResult(ByteStringer.wrap(result));
    }

    ByteString.Output stateStream = ByteString.newOutput();
    proc.serializeStateData(stateStream);
    if (stateStream.size() > 0) {
      builder.setStateData(stateStream.toByteString());
    }

    builder.build().writeDelimitedTo(out);
  }

  /**
   * Helper to deserialize the procedure taken from a byte array.
   * Used by ProcedureStore implementations.
   */
  @InterfaceAudience.Private
  public static Procedure deserializeProcedure(long procId, byte[] data)
      throws IOException {
    return deserializeProcedure(procId, new ByteArrayInputStream(data));
  }

  /**
   * Helper to deserialize the procedure taken from an input stream.
   * Used by ProcedureStore implementations.
   */
  @InterfaceAudience.Private
  public static Procedure deserializeProcedure(long procId, InputStream stream)
      throws IOException {
    ProcedureProtos.Procedure proto = ProcedureProtos.Procedure.parseDelimitedFrom(stream);

    // Procedure from class name
    Procedure proc = Procedure.newInstance(proto.getClassName());

    // set fields
    assert procId == proto.getProcId();
    proc.setProcId(procId);
    proc.setState(proto.getState());
    proc.setStartTime(proto.getStartTime());
    proc.setLastUpdate(proto.getLastUpdate());

    if (proto.hasParentId()) {
      proc.setParentProcId(proto.getParentId());
    }

    if (proto.hasOwner()) {
      proc.setOwner(proto.getOwner());
    }

    if (proto.hasTimeout()) {
      proc.setTimeout(proto.getTimeout());
    }

    if (proto.getStackIdCount() > 0) {
      proc.setStackIndexes(proto.getStackIdList());
    }

    if (proto.hasException()) {
      assert proc.getState() == ProcedureState.FINISHED;
      proc.setFailure(RemoteProcedureException.fromProto(proto.getException()));
    }

    if (proto.hasResult()) {
      proc.setResult(proto.getResult().toByteArray());
    }

    // we want to call deserialize even when the stream is empty, mainly for testing.
    proc.deserializeStateData(proto.getStateData().newInput());

    return proc;
  }
}