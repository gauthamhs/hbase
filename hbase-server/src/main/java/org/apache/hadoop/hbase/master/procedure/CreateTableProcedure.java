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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

@InterfaceAudience.Private
public class CreateTableProcedure
    extends StateMachineProcedure<CreateTableProcedure.CreateTableState>
    implements TableProcedureInterface {
  enum CreateTableState {
    CREATE_FS_LAYOUT,
    ADD_TABLE_TO_META,
    UPDATE_TABLE_DESC_CACHE,
    COMPLETED,
  }

  private AtomicBoolean aborted = new AtomicBoolean(false);
  private TableName tableName;

  public CreateTableProcedure() {}

  public CreateTableProcedure(final TableName tableName) {
    this.tableName = tableName;
  }

  protected boolean executeFromState(CreateTableState state) {
    if (state == null) {
      state = CreateTableState.CREATE_FS_LAYOUT;
      setNextState(state);
    }

    try {
      switch (state) {
        case CREATE_FS_LAYOUT:
          createFsLayout();
          setNextState(CreateTableState.ADD_TABLE_TO_META);
          break;
        case ADD_TABLE_TO_META:
          addTableToMeta();
          setNextState(CreateTableState.UPDATE_TABLE_DESC_CACHE);
          break;
        case UPDATE_TABLE_DESC_CACHE:
          updateTableDescCache();
          setNextState(CreateTableState.COMPLETED);
          return true;
      }
    } catch (Exception e) {
      setFailure("master-create-table", e);
    }
    return false;
  }

  protected void rollbackState(final CreateTableState state) {
    switch (state) {
      case CREATE_FS_LAYOUT:
        break;
      case ADD_TABLE_TO_META:
        break;
      case UPDATE_TABLE_DESC_CACHE:
        break;
    }
  }

  protected CreateTableState getState(final int stateId) {
    return CreateTableState.values()[stateId];
  }

  private boolean setNextState(final CreateTableState state) {
    if (aborted.get()) {
      setAbortFailure();
      return false;
    } else {
      setNextState(state.ordinal());
      return true;
    }
  }

  @Override
  public boolean requiresTableLock() {
    return true;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public int getType() {
    return MasterProcedureConstants.TABLE_PROCEDURE_TYPE;
  }

  @Override
  public void abort() {
    aborted.set(true);
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);
    ProtobufUtil.toProtoTableName(tableName).writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);
    tableName = ProtobufUtil.toTableName(HBaseProtos.TableName.parseDelimitedFrom(stream));
  }

  private void createFsLayout() {
  }

  private void addTableToMeta() {
  }

  private void updateTableDescCache() {
  }
}
