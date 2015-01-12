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

import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
public class CreateTableProcedure
    extends StateMachineProcedure<MasterProcedureEnv, CreateTableProcedure.CreateTableState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(CreateTableProcedure.class);

  enum CreateTableState {
    PRE_CREATE,
    CREATE_FS_LAYOUT,
    ADD_TABLE_TO_META,
    ASSIGN_REGIONS,
    UPDATE_TABLE_DESC_CACHE,
    POST_CREATE,
  }

  private AtomicBoolean aborted = new AtomicBoolean(false);
  private HTableDescriptor hTableDescriptor;
  private List<HRegionInfo> newRegions;
  private User user;

  public CreateTableProcedure() {}

  public CreateTableProcedure(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions)
      throws IOException {
    this.hTableDescriptor = hTableDescriptor;
    this.newRegions = newRegions != null ? Lists.newArrayList(newRegions) : null;
    this.user = env.getRequestUser();
  }

  protected Flow executeFromState(final MasterProcedureEnv env, CreateTableState state) {
    if (state == null) {
      state = CreateTableState.PRE_CREATE;
      setNextState(state);
    }

    try {
      switch (state) {
        case PRE_CREATE:
          preCreate(env);
          setNextState(CreateTableState.CREATE_FS_LAYOUT);
          break;
        case CREATE_FS_LAYOUT:
          createFsLayout(env);
          setNextState(CreateTableState.ADD_TABLE_TO_META);
          break;
        case ADD_TABLE_TO_META:
          addTableToMeta(env);
          setNextState(CreateTableState.ASSIGN_REGIONS);
          break;
        case ASSIGN_REGIONS:
          assignRegions(env);
          setNextState(CreateTableState.UPDATE_TABLE_DESC_CACHE);
          break;
        case UPDATE_TABLE_DESC_CACHE:
          updateTableDescCache(env);
          setNextState(CreateTableState.POST_CREATE);
          break;
        case POST_CREATE:
          postCreate(env);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      LOG.error("Error trying to create the table " + getTableName(), e);
      setFailure("master-create-table", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  protected void rollbackState(final MasterProcedureEnv env, final CreateTableState state) {
    try {
      switch (state) {
        case POST_CREATE:
          break;
        case UPDATE_TABLE_DESC_CACHE:
          DeleteTableProcedure.deleteTableDescriptorCache(env, getTableName());
          break;
        case ASSIGN_REGIONS:
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          break;
        case ADD_TABLE_TO_META:
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), newRegions);
          break;
        case CREATE_FS_LAYOUT:
          DeleteTableProcedure.deleteFromFs(env, getTableName(), newRegions, false);
          break;
        case PRE_CREATE:
          env.deleteTable(getTableName());
          // TODO-MAYBE: call the deleteTable coprocessor event?
          break;
      }
    } catch (Exception e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step=" + state + " table=" + getTableName(), e);
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
  public TableName getTableName() {
    return hTableDescriptor.getTableName();
  }

  @Override
  public int getType() {
    return MasterProcedureConstants.TABLE_PROCEDURE_TYPE;
  }

  @Override
  public void abort(final MasterProcedureEnv env) {
    aborted.set(true);
  }

  @Override
  public String toString() {
    return "CreateTableProcedure(table=" + getTableName() + ") id=" + getProcId();
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    // TODO: Convert to protobuf MasterProcedure.proto
    super.serializeStateData(stream);

    MasterProcedureProtos.CreateTableState.Builder state =
      MasterProcedureProtos.CreateTableState.newBuilder()
        .setTableSchema(hTableDescriptor.convert());
    if (newRegions != null) {
      for (HRegionInfo hri: newRegions) {
        state.addRegionInfo(HRegionInfo.convert(hri));
      }
    }
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.CreateTableState state =
      MasterProcedureProtos.CreateTableState.parseDelimitedFrom(stream);
    hTableDescriptor = HTableDescriptor.convert(state.getTableSchema());
    if (state.getRegionInfoCount() == 0) {
      newRegions = null;
    } else {
      newRegions = new ArrayList<HRegionInfo>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri: state.getRegionInfoList()) {
        newRegions.add(HRegionInfo.convert(hri));
      }
    }
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    return env.getProcedureQueue().tryAcquireTableWrite(getTableName());
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableWrite(getTableName());
  }

  private void preCreate(final MasterProcedureEnv env) throws IOException {
    final TableName tableName = getTableName();
    if (!env.createTable(tableName)) {
      throw new TableExistsException(tableName);
    }

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final HRegionInfo[] regions = newRegions == null ? null :
        newRegions.toArray(new HRegionInfo[newRegions.size()]);
      cpHost.preCreateTableHandler(hTableDescriptor, regions);
    }
  }

  private void createFsLayout(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tempdir = mfs.getTempDir();

    // 1. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    TableDescriptor underConstruction = new TableDescriptor(this.hTableDescriptor);
    final Path tempTableDir = FSUtils.getTableDir(tempdir, getTableName());
    ((FSTableDescriptors)(env.getMasterServices().getTableDescriptors()))
        .createTableDescriptorForTableDirectory(
          tempTableDir, underConstruction, false);

    // 2. Create Regions
    newRegions = handleCreateHdfsRegions(env, tempdir, getTableName());

    // 3. Move Table temp directory to the hbase root location
    final Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), getTableName());
    if (!mfs.getFileSystem().rename(tempTableDir, tableDir)) {
      throw new IOException("Unable to move table from temp=" + tempTableDir +
        " to hbase root=" + tableDir);
    }
  }

  /**
   * Create the on-disk structure for the table, and returns the regions info.
   * @param tableRootDir directory where the table is being created
   * @param tableName name of the table under construction
   * @return the list of regions created
   */
  protected List<HRegionInfo> handleCreateHdfsRegions(final MasterProcedureEnv env,
      final Path tableRootDir, final TableName tableName) throws IOException {
    HRegionInfo[] regions = newRegions != null ?
      newRegions.toArray(new HRegionInfo[newRegions.size()]) : null;
    return ModifyRegionUtils.createRegions(env.getMasterConfiguration(),
        tableRootDir, hTableDescriptor, regions, null);
  }

  private void addTableToMeta(final MasterProcedureEnv env) throws IOException {
    if (newRegions != null && newRegions.size() > 0) {
      // Add regions to META
      addRegionsToMeta(env, newRegions);
      // Add replicas if needed
      newRegions = addReplicas(env, hTableDescriptor, newRegions);

      // Setup replication for region replicas if needed
      if (hTableDescriptor.getRegionReplication() > 1) {
        ServerRegionReplicaUtil.setupRegionReplicaReplication(env.getMasterConfiguration());
      }
    }
  }

  private void assignRegions(final MasterProcedureEnv env) throws IOException {
    // Trigger immediate assignment of the regions in round-robin fashion
    final AssignmentManager assignmentManager = env.getMasterServices().getAssignmentManager();
    ModifyRegionUtils.assignRegions(assignmentManager, newRegions);

    // Enable table
    assignmentManager.getTableStateManager()
      .setTableState(getTableName(), TableState.State.ENABLED);
  }

  /**
   * Create any replicas for the regions (the default replicas that was
   * already created is passed to the method)
   * @param hTableDescriptor descriptor to use
   * @param regions default replicas
   * @return the combined list of default and non-default replicas
   */
  protected List<HRegionInfo> addReplicas(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor,
      final List<HRegionInfo> regions) {
    int numRegionReplicas = hTableDescriptor.getRegionReplication() - 1;
    if (numRegionReplicas <= 0) {
      return regions;
    }
    List<HRegionInfo> hRegionInfos =
        new ArrayList<HRegionInfo>((numRegionReplicas+1)*regions.size());
    for (int i = 0; i < regions.size(); i++) {
      for (int j = 1; j <= numRegionReplicas; j++) {
        hRegionInfos.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(i), j));
      }
    }
    hRegionInfos.addAll(regions);
    return hRegionInfos;
  }

  /**
   * Add the specified set of regions to the hbase:meta table.
   */
  protected void addRegionsToMeta(final MasterProcedureEnv env,
      final List<HRegionInfo> regionInfos) throws IOException {
    MetaTableAccessor.addRegionsToMeta(env.getMasterServices().getConnection(),
      regionInfos, hTableDescriptor.getRegionReplication());
  }

  private void updateTableDescCache(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().get(getTableName());
  }

  private void postCreate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final HRegionInfo[] regions = newRegions == null ? null :
        newRegions.toArray(new HRegionInfo[newRegions.size()]);
      // TODO: the user must be the one taken in the constructor
      User user = env.getRequestUser();
      user.runAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          cpHost.postCreateTableHandler(hTableDescriptor, regions);
          return null;
        }
      });
    }
  }
}
