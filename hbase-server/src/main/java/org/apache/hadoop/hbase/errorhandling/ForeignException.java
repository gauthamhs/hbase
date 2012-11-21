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
package org.apache.hadoop.hbase.errorhandling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.ForeignExceptionMessage;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.GenericExceptionMessage;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.StackTraceElementMessage;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A Foreign Thread Exception is an exception from a separate thread or process.  A 
 * ForeignException is "local" if it comes from a different thread but from the same
 * process. "local" FTE's  will have its {@link #getCause()} exception non-null with a full
 * instance of the original exception and {@link #getSource()} will be null.  If the
 * {@link #getCause()} exception is null, it is a "remote" exception (from a different process)
 * and {@link #getSource()} should be non-null.
 * <p>
 * ForeignExceptions are sent to "remote" peers to signal an abort in the face of partial
 * failures.  When serialized for transmission we encode using Protobufs to ensure version
 * compatibility.
 * <p>
 * An potential use of ForeignExceptions is to have a remote process inject an exceptions and their
 * information to a local thread.
 * <p>
 * Foreign exceptions have their stacks overridden when they are deserialized with the stacktrace info
 * from the original exception on the original source.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public class ForeignException extends IOException {
  /**
   * Name of the exception source. null if the source is "local".
   */
  private final String source;

  // Extensibility mechanism, in protobuf but not yet used.
  private final byte[] errorInfo;

  private ForeignException(String msg, Throwable cause, String source, byte[] errorInfo) {
    super(msg, cause);
    this.source = source;
    this.errorInfo = errorInfo;
  }

  public ForeignException(Throwable cause) {
    this(null, cause, null, null);
  }

  public ForeignException(String msg) {
    this(msg, null, null, null);
  }

  public ForeignException(String message, Throwable cause) {
    this(message, cause, null, null);
  }

  public ForeignException(Exception cause, byte[] errorInfo) {
    this(null, cause, null, errorInfo);
  }

  public byte[] getErrorInfo() {
     return errorInfo;
  }

  public String getSource() {
    return source;
  }

  /**
   * An foreign exception is remote if it comes from a foreign thread.  In these cases, it will
   * have a non-null source.
   * @return true if remote, false if local
   */
  public boolean isRemote() {
    return source != null;
  }

  @Override
  public String toString() {
    if (getCause() == null) {
      return getClass().getName() + ": from '"+ getSource() +"' originally " +
          getLocalizedMessage();
    } else {
      return "Local ForeignThreadException from " + getLocalizedMessage();
    }
  }

  /**
   * Convert an arbitrary exception from a particular source into a protobuf
   * {@link ForeignExceptionMessage}
   * <p>
   * package exposed for testing
   *
   * @param srcName name of the process/thread the exception originally came from.
   * @param e Exception for serialization.
   * @return an {@link ForeignExceptionMessage} protobuf
    */
  static ForeignExceptionMessage toForeignExceptionMessage(String srcName, Exception e){
    GenericExceptionMessage.Builder gemBuilder = GenericExceptionMessage.newBuilder();
    gemBuilder.setClassName(e.getClass().getName());
    gemBuilder.setMessage(e.getMessage());
    // set the stack trace, if there is one
    List<StackTraceElementMessage> stack = toStackTraceElementMessages(e.getStackTrace());
    if (stack != null) {
      gemBuilder.addAllTrace(stack);
    }
    GenericExceptionMessage payload = gemBuilder.build();

    ForeignExceptionMessage.Builder exception = ForeignExceptionMessage.newBuilder();
    exception.setGenericException(payload).setSource(srcName);
    return exception.build();
  }

  /**
   * Convert a stack trace to list of {@link StackTraceElement}.
   * @param stackTrace the stack trace to convert to protobuf message
   * @return <tt>null</tt> if the passed stack is <tt>null</tt>.
   */
  private static List<StackTraceElementMessage> toStackTraceElementMessages(
      StackTraceElement[] trace) {
    // if there is no stack trace, ignore it and just return the message
    if (trace == null) return null;
    // build the stack trace for the message
    List<StackTraceElementMessage> pbTrace =
        new ArrayList<StackTraceElementMessage>(trace.length);
    for (StackTraceElement elem : trace) {
      StackTraceElementMessage.Builder stackBuilder = StackTraceElementMessage.newBuilder();
      stackBuilder.setDeclaringClass(elem.getClassName());
      stackBuilder.setFileName(elem.getFileName());
      stackBuilder.setLineNumber(elem.getLineNumber());
      stackBuilder.setMethodName(elem.getMethodName());
      pbTrace.add(stackBuilder.build());
    }
    return pbTrace;
  }

  /**
   * Unwind the generic exception from the wrapping done with a generic error message by
   * {@link #buildExceptionMessage(ForeignException)}.
   * <p>
   * package exposed for testing
   *
   * @param remoteCause message to inspect
   * @return the original phase and the error, if they are valid, <tt>null</tt> otherwise
   */
  static ForeignException unwind(ForeignExceptionMessage remoteCause) {
    GenericExceptionMessage gem = remoteCause.getGenericException();
    String reason = (gem.getMessage()==null) ? "" : (": "+ gem.getMessage());
    String msg = gem.getClassName() + reason;

    ForeignException e = new ForeignException(msg, null, remoteCause.getSource(),
        gem.getErrorInfo().toByteArray());
    StackTraceElement [] trace = toStack(gem.getTraceList());
    if (trace != null) e.setStackTrace(trace);
    return e;
  }

  /**
   * Converts an ForeignThreadExceptionException to a array of bytes.
   * @param source the name of the external exception source
   * @param ee the "local" external exception (local)
   * @return protobuf serialized version of ForeignThreadException
   */
  public static byte[] serialize(String source, ForeignException ee) {
    ForeignExceptionMessage eem = ForeignException.toForeignExceptionMessage(source, ee);
    return eem.toByteArray();
  }

  /**
   * Takes a series of bytes and tries to generate an ForeignThreadException instance for it.
   * @param bytes
   * @return the ExternalExcpetion instance
   * @throws InvalidProtocolBufferException if there was deserialization problem this is thrown.
   */
  public static ForeignException deserialize(byte[] bytes) throws InvalidProtocolBufferException {
    // figure out the data we need to pass
    ForeignExceptionMessage eem = ForeignExceptionMessage.parseFrom(bytes);
    return ForeignException.unwind(eem);
  }

  /**
   * Unwind a serialized array of {@link StackTraceElementMessage}s to a
   * {@link StackTraceElement}s.
   * @param traceList list that was serialized
   * @return the deserialized list or <tt>null</tt> if it couldn't be unwound (e.g. wasn't set on
   *         the sender).
   */
  private static StackTraceElement[] toStack(List<StackTraceElementMessage> traceList) {
    if (traceList == null || traceList.size() == 0) {
      return null;
    }
    StackTraceElement[] trace = new StackTraceElement[traceList.size()];
    for (int i = 0; i < traceList.size(); i++) {
      StackTraceElementMessage elem = traceList.get(i);
      trace[i] = new StackTraceElement(
          elem.getDeclaringClass(), elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
    }
    return trace;
  }
}