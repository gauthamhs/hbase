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
package org.apache.hadoop.hbase.server.snapshot.task;

import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionCheckable;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

/**
 * General snapshot operation taken on a regionserver
 */
public abstract class SnapshotTask implements ForeignExceptionCheckable, Callable<Void>{

  protected final SnapshotDescription snapshot;
  protected final ForeignExceptionDispatcher errorMonitor;

  /**
   * @param snapshot Description of the snapshot we are going to operate on
   * @param monitor listener interested in failures to the snapshot caused by this operation
   */
  public SnapshotTask(SnapshotDescription snapshot, ForeignExceptionDispatcher monitor) {
    this.snapshot = snapshot;
    this.errorMonitor = monitor;
  }

  public void snapshotFailure(String message, Exception e) {
    ForeignException ee = new ForeignException(message, e);
    errorMonitor.receiveError(message, ee, snapshot);
  }

  @Override
  public void rethrowException() throws ForeignException {
    this.errorMonitor.rethrowException();
  }

  @Override
  public boolean hasException() {
    return this.errorMonitor.hasException();
  }

  @Override
  public ForeignException getException() {
    return this.errorMonitor.getException();
  }
}
