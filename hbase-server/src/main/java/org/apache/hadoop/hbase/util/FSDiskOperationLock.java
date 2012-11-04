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
package org.apache.hadoop.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.protobuf.generated.FSProtos.DiskOperationLock;
import org.apache.hadoop.hbase.HConstants;

@InterfaceAudience.Private
public class FSDiskOperationLock {
  public static final String OPERATION_LOCK_FILE = ".operation.lock";

  public enum Type {
    CREATE_TABLE,
    DELETE_TABLE,
    RESTORE_TABLE,
    CLONE_TABLE,
  }

  private String source = null;
  private final Type type;

  public FSDiskOperationLock(final Type type) {
    this.type = type;
  }

  public FSDiskOperationLock(final Type type, final String source) {
    this.type = type;
    this.source = source;
  }

  public String getSource() {
    return this.source;
  }

  public Type getType() {
    return this.type;
  }

  /**
   * Write the Operation Lock on disk
   * @parms fs {@link FileSystem} to store the lock file
   * @parms path lock file
   * @throws IOException if the file cannot be written
   */
  public void write(final FileSystem fs, final Path path) throws IOException {
    DiskOperationLock.Builder builder = DiskOperationLock.newBuilder();
    if (this.source != null) builder.setSource(this.source);
    switch (type) {
      case CREATE_TABLE:
        builder.setOperation(DiskOperationLock.Operation.CREATE_TABLE);
        break;
      case DELETE_TABLE:
        builder.setOperation(DiskOperationLock.Operation.DELETE_TABLE);
        break;
      case RESTORE_TABLE:
        builder.setOperation(DiskOperationLock.Operation.RESTORE_TABLE);
        break;
      case CLONE_TABLE:
        builder.setOperation(DiskOperationLock.Operation.CLONE_TABLE);
        break;
    }

    FSDataOutputStream stream = fs.create(path);
    try {
      builder.build().writeTo(stream);
    } finally {
      stream.close();
    }
  }

  /**
   * Read the Disk Operation Lock
   * @parms fs {@link FileSystem} of that contains the lock file
   * @parms path lock file
   * @return the operation lock object
   * @throws IOException if the file cannot be read
   */
  public static FSDiskOperationLock read(final FileSystem fs, final Path path)
      throws IOException {
    FSDataInputStream stream = fs.open(path);
    try {
      FSDiskOperationLock operationLock = null;

      DiskOperationLock proto = DiskOperationLock.parseFrom(stream);
      switch (proto.getOperation()) {
        case CREATE_TABLE:
          operationLock = new FSDiskOperationLock(Type.CREATE_TABLE);
          break;
        case DELETE_TABLE:
          operationLock = new FSDiskOperationLock(Type.DELETE_TABLE);
          break;
        case RESTORE_TABLE:
          operationLock = new FSDiskOperationLock(Type.RESTORE_TABLE);
          break;
        case CLONE_TABLE:
          operationLock = new FSDiskOperationLock(Type.CLONE_TABLE);
          break;
        default:
          throw new IllegalArgumentException("Invalid lock operation");
      }

      if (proto.hasSource()) {
        operationLock.source = proto.getSource();
      }

      return operationLock;
    } finally {
      stream.close();
    }
  }

  public static Path getTableOperationLockFile(final Path tableDir) {
    return new Path(tableDir, FSDiskOperationLock.OPERATION_LOCK_FILE);
  }
}
