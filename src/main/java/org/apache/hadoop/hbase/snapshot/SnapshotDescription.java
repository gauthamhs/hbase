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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

public class SnapshotDescription implements Writable {
  private HBaseProtos.SnapshotDescription proto;

  public SnapshotDescription() {
  }

  public SnapshotDescription(final HBaseProtos.SnapshotDescription proto) {
    this.proto = proto;
  }

  public String getName() {
    return this.proto.getName();
  }

  public HBaseProtos.SnapshotDescription getProto() {
    return this.proto;
  }

  public HBaseProtos.SnapshotDescription.Type getType() {
    return this.proto.getType();
  }

  public String getTable() {
    return this.proto.getTable();
  }

  public boolean hasTable() {
    return this.proto.hasTable();
  }

  public long getCreationTime() {
    return this.proto.getCreationTime();
  }

  public int getVersion() {
    return this.proto.getVersion();
  }

  public String toString() {
    return this.proto.toString();
  }
  
  // Writable
  /**
   * <em> INTERNAL </em> This method is a part of {@link Writable} interface 
   * and is used for de-serialization of the HTableDescriptor over RPC
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.proto = HBaseProtos.SnapshotDescription.parseFrom(Bytes.readByteArray(in));
  }

  /**
   * <em> INTERNAL </em> This method is a part of {@link Writable} interface 
   * and is used for serialization of the HTableDescriptor over RPC
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.proto.toByteArray());
  }
}
