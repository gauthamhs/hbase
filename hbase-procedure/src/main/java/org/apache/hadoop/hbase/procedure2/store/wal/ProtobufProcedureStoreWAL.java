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

package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.util.ByteStringer;

import com.google.protobuf.ByteString;

// TODO: Merge this with base since now header/footer is protobuf?
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProtobufProcedureStoreWAL extends BaseProcedureStoreWAL {
  private final Log LOG = LogFactory.getLog(ProtobufProcedureStoreWAL.class);

  private final Path logDir;

  public ProtobufProcedureStoreWAL(final FileSystem fs, final Path logDir) {
    super(fs);
    this.logDir = logDir;
  }

  @Override
  protected Path createNewFile(final long logId) throws IOException {
    return new Path(logDir, String.format("state-%020d.log", logId));
  }

  @Override
  protected FileStatus[] getLogFiles() throws IOException {
    return fs.listStatus(logDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("state-") && path.getName().endsWith(".log");
      }
    });
  }

  @Override
  protected void serializeProcedure(final OutputStream out, final Procedure proc)
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

    int[] stackIds = proc.getStackIndexes();
    if (stackIds != null) {
      for (int i = 0; i < stackIds.length; ++i) {
        builder.addStackId(stackIds[i]);
      }
    }

    if (proc.hasException()) {
      RemoteProcedureException exception = proc.getException();
      builder.setException(RemoteProcedureException.toProto(exception.getSource(), exception.getCause()));
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

  @Override
  protected Procedure deserializeProcedure(long procId, byte[] data) throws IOException {
    return deserializeProcedure(procId, new ByteArrayInputStream(data));
  }

  private Procedure deserializeProcedure(long procId, InputStream stream) throws IOException {
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