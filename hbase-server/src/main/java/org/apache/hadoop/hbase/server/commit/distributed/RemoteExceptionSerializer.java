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
package org.apache.hadoop.hbase.server.commit.distributed;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.GenericExceptionMessage;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.StackTraceElement;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.TaskTimeoutMessage;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Helper class to deal with wrapping exceptions on the local node into a
 * {@link RemoteFailureException} that can be passed to other nodes and then unwrapping the message
 * on the remote nodes.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RemoteExceptionSerializer {

  private static final Log LOG = LogFactory.getLog(RemoteExceptionSerializer.class);
  private final String nodeName;

  public RemoteExceptionSerializer(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Helper method for {@link #buildRemoteException(String, OperationAttemptTimeoutException)}
   * @param cause method to serialize
   * @return {@link RemoteFailureException} that can be passed to other nodes
   */
  public RemoteFailureException buildRemoteException(OperationAttemptTimeoutException cause) {
    return buildRemoteException(nodeName, cause);
  }

  /**
   * Build a {@link GenericExceptionMessage} from a local message that was thrown at the given
   * {@link CommitPhase}.
   * @param phase phase where the exception was thrown
   * @param cause exception that was thrown locally
   * @return a {@link RemoteFailureException} to pass to other nodes
   */
  public RemoteFailureException buildRemoteException(CommitPhase phase,
      DistributedCommitException cause) {
    GenericExceptionMessage payload = buildGenericMessage(phase, cause);
    return buildRemoteFailureException(payload);
  }

  /**
   * Build a remote exception to use <b>locally</b> when the remote exception cannot be read. This
   * should not be passed to other nodes, but can still be unwrapped with
   * {@link #unwind(RemoteFailureException)}.
   * @param localException local exception to wrap
   * @return a {@link RemoteFailureException} to pass to internal methods on the same node
   */
  public RemoteFailureException buildRemoteException(Exception localException){
    GenericExceptionMessage payload = buildGenericMessage(localException, null);
    byte[] phase = Bytes.toBytes((int) -1);
    payload = buildGenericMessage(phase, payload);
    return buildRemoteFailureException(payload);
  }

  /**
   * @param payload {@link GenericExceptionMessage} message to pass along
   * @return a {@link RemoteFailureException} to pass to other nodes
   */
  private RemoteFailureException buildRemoteFailureException(GenericExceptionMessage payload) {
    RemoteFailureException.Builder exception = RemoteFailureException.newBuilder();
    exception.setGenericException(payload);
    return finish(exception, nodeName);
  }

  private static GenericExceptionMessage buildGenericMessage(byte[] phase,
      GenericExceptionMessage cause) {
    ByteString bytes = cause.toByteString();
    // copy in the bytes for each into the new byte paylod
    byte[] data = new byte[phase.length + bytes.size()];
    System.arraycopy(phase, 0, data, 0, phase.length);
    bytes.copyTo(data, phase.length);
    return GenericExceptionMessage.newBuilder().setErrorInfo(ByteString.copyFrom(data)).build();
  }

  /**
   * Build a {@link GenericExceptionMessage} from a local message that was thrown at the given
   * {@link CommitPhase}.
   * @param phase phase where the exception was thrown
   * @param cause exception that was thrown locally
   * @return a {@link GenericExceptionMessage} to pass to other nodes
   */
  private static GenericExceptionMessage buildGenericMessage(CommitPhase phase,
      DistributedCommitException cause) {
    // create a generic message
    GenericExceptionMessage baseMessage = buildGenericMessage(cause, cause.getExceptionInfo());
    // dump the commit phase into a byte array too
    byte[] phaseBytes = Bytes.toBytes(phase.getNumber());
    return buildGenericMessage(phaseBytes, baseMessage);
  }

  /**
   * Build a {@link GenericExceptionMessage} that from an {@link Exception} and some some supporting
   * bytes
   * @param e exception to serialize
   * @param data extra error info to wrap into this exception message
   * @return a {@link GenericExceptionMessage} that can be unwound with
   *         {@link #unwind(RemoteFailureException)}
   */
  private static GenericExceptionMessage buildGenericMessage(Exception e, byte[] data) {
    GenericExceptionMessage.Builder exception = GenericExceptionMessage.newBuilder();
    exception.setClassName(e.getClass().toString());

    // set the stack trace, if there is one
    List<StackTraceElement> stack = buildStackTraceMessage(e.getStackTrace());
    if (stack != null) {
      exception.addAllTrace(stack);
    }
    // set the error info, if we found any
    if (data != null) exception.setErrorInfo(ByteString.copyFrom(data));

    return exception.build();
  }

  /**
   * Build a {@link RemoteFailureException} for a node that has a timeout
   * @param sourceNodeName name of the node in the distributed operation
   * @param cause the source timeout exception
   * @return a {@link RemoteFailureException} that can be serialized to other tasks in the operation
   */
  public static RemoteFailureException buildRemoteException(String sourceNodeName,
      OperationAttemptTimeoutException cause) {
    RemoteFailureException.Builder builder = RemoteFailureException.newBuilder();
    TaskTimeoutMessage timeout = TaskTimeoutMessage.newBuilder()
        .setAllowed(cause.getMaxAllowedOperationTime()).setStart(cause.getStart())
        .setEnd(cause.getEnd()).build();
    builder.setTimeout(timeout);
    return finish(builder, sourceNodeName);
  }

  /**
   * Convert a stack trace to list of {@link StackTraceElement}.
   * @param stackTrace the stack trace to convert to protobuf message
   * @return <tt>null</tt> if the passed stack is <tt>null</tt>.
   */
  private static List<StackTraceElement> buildStackTraceMessage(
      java.lang.StackTraceElement[] stackTrace) {
    java.lang.StackTraceElement[] trace = stackTrace;
    // if there is no stack trace, ignore it and just return the message
    if (trace == null) return null;
    // build the stack trace for the message
    List<StackTraceElement> pbTrace = new ArrayList<StackTraceElement>(trace.length);
    for (java.lang.StackTraceElement elem : trace) {
      StackTraceElement.Builder stackBuilder = StackTraceElement.newBuilder();
      stackBuilder.setDeclaringClass(elem.getClassName());
      stackBuilder.setFileName(elem.getFileName());
      stackBuilder.setLineNumber(elem.getLineNumber());
      stackBuilder.setMethodName(elem.getMethodName());
      pbTrace.add(stackBuilder.build());
    }
    return pbTrace;
  }

  /**
   * Simple helper method to set the common parameters for a {@link RemoteFailureException}
   * @param builder builder for the {@link RemoteFailureException} to pass to other nodes
   * @param nodeName name of the source node that wants to throw the error
   * @return a {@link RemoteFailureException} that can be passed to other nodes in the operation
   */
  private static RemoteFailureException finish(RemoteFailureException.Builder builder,
      String nodeName) {
    return builder.setSource(nodeName).build();
  }

  /**
   * Unwind the remote exception to get the root cause and log that exception
   * @param remoteCause
   */
  public static void logRemoteCause(RemoteFailureException remoteCause) {
    Exception cause = getTimeoutException(remoteCause);
    // if we got an exception, it was a timeout
    if (cause != null) {
      LOG.error("Task failure on:" + remoteCause.getSource() + " due to timeout.", cause);
      return;
    }
    // otherwise it was a specific exception that we need to unwind
    Pair<CommitPhase, DistributedCommitException> e = unwind(remoteCause);
    if (e == null) {
      LOG.error("Got an unknown remote exception: " + remoteCause);
      return;
    }
    LOG.error("Got a remote exception during " + e.getFirst(), e.getSecond());
  }

  /**
   * @param remoteCause remote message in which to look for a timeout
   * @return a {@link RemoteTaskTimeoutException} with information about the timeout, if there way a
   *         timeout, <tt>null</tt> otherwise.
   */
  static RemoteTaskTimeoutException getTimeoutException(RemoteFailureException remoteCause) {
    if (remoteCause.hasTimeout()) {
      TaskTimeoutMessage msg = remoteCause.getTimeout();
      return new RemoteTaskTimeoutException(remoteCause.getSource(), msg.getStart(), msg.getEnd(),
          msg.getAllowed());
    }
    return null;
  }

  /**
   * Unwind the generic exception from the wrapping done with a generic error message by
   * {@link #buildRemoteException(CommitPhase, DistributedCommitException)}.
   * @param remoteCause message to inspect
   * @return the original phase and the error, if they are valid, <tt>null</tt> otherwise
   */
  public static Pair<CommitPhase, DistributedCommitException> unwind(
      RemoteFailureException remoteCause) {
    GenericExceptionMessage msg = remoteCause.getGenericException();
   ByteBuffer data = msg.getErrorInfo().asReadOnlyByteBuffer();
   CommitPhase phase = CommitPhase.valueOf(Bytes.readInt(data));
    try {
      msg = GenericExceptionMessage.parseFrom(ByteString.copyFrom(data));
    } catch (InvalidProtocolBufferException e1) {
      LOG.error("Could not read in exception message", e1);
      return null;
    }
    DistributedCommitException e = new DistributedCommitException(msg.getClassName()
        + " thrown because: " + msg.getMessage(), msg.getErrorInfo().toByteArray());
   java.lang.StackTraceElement [] trace = unwind(msg.getTraceList());
    if (trace != null) e.setStackTrace(trace);
    return new Pair<CommitPhase, DistributedCommitException>(phase, e);
  }

  /**
   * Unwind a serialized array of {@link java.lang.StackTraceElement} from a
   * {@link StackTraceElement}.
   * @param traceList list that was serialized
   * @return the deserialized list or <tt>null</tt> if it couldn't be unwound (e.g. wasn't set on
   *         the sender).
   */
  private static java.lang.StackTraceElement[] unwind(List<StackTraceElement> traceList) {
    if (traceList == null || traceList.size() == 0) return null;
    java.lang.StackTraceElement[] trace = new java.lang.StackTraceElement[traceList.size()];
    for (int i = 0; i < traceList.size(); i++) {
      StackTraceElement elem = traceList.get(i);
      trace[i] = new java.lang.StackTraceElement(
          elem.getDeclaringClass(), elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
    }
    return trace;
  }
}