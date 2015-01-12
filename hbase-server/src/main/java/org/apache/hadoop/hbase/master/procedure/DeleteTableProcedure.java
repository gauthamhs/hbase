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
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;

@InterfaceAudience.Private
public class DeleteTableProcedure
    extends StateMachineProcedure<MasterProcedureEnv, DeleteTableProcedure.DeleteTableState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(DeleteTableProcedure.class);

  enum DeleteTableState {
    PRE_DELETE,
    DELETE_TABLE_DESC_CACHE,
    UNASSIGN_REGIONS,
    DELETE_TABLE_FROM_META,
    DELETE_FS_LAYOUT,
    POST_DELETE,
  }

  private List<HRegionInfo> regions;
  private TableName tableName;

  public DeleteTableProcedure() {}

  public DeleteTableProcedure(final TableName tableName) {
    this.tableName = tableName;
  }

  protected Flow executeFromState(final MasterProcedureEnv env, DeleteTableState state) {
    if (state == null) {
      state = DeleteTableState.PRE_DELETE;
      setNextState(state);
    }

    try {
      switch (state) {
        case PRE_DELETE:
          // TODO: Move out... in the acquireLock()
          regions = getRegionsFromMeta(env, getTableName());
          waitRegionInTransition(env, regions);

          preDelete(env);
          setNextState(DeleteTableState.DELETE_TABLE_FROM_META);
          break;
        case DELETE_TABLE_FROM_META:
          regions = getRegionsFromMeta(env, getTableName());
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), regions);
          setNextState(DeleteTableState.DELETE_FS_LAYOUT);
          break;
        case DELETE_FS_LAYOUT:
          DeleteTableProcedure.deleteFromFs(env, getTableName(), regions, false);
          setNextState(DeleteTableState.DELETE_TABLE_DESC_CACHE);
          break;
        case DELETE_TABLE_DESC_CACHE:
          DeleteTableProcedure.deleteTableDescriptorCache(env, getTableName());
          setNextState(DeleteTableState.UNASSIGN_REGIONS);
          break;
        case UNASSIGN_REGIONS:
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          setNextState(DeleteTableState.POST_DELETE);
          break;
        case POST_DELETE:
          postDelete(env);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      LOG.warn("Error trying to delete the table " + getTableName(), e);
    }
    return Flow.HAS_MORE_STATE;
  }

  protected void rollbackState(final MasterProcedureEnv env, final DeleteTableState state) {
    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException();
  }

  protected DeleteTableState getState(final int stateId) {
    return DeleteTableState.values()[stateId];
  }

  private boolean setNextState(final DeleteTableState state) {
    setNextState(state.ordinal());
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
  public void abort(final MasterProcedureEnv env) {
    // TODO: We may abort if the procedure is not started yet.
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    return env.getProcedureQueue().tryAcquireTableWrite(getTableName());
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableWrite(getTableName());
  }

  @Override
  public String toString() {
    return "DeleteTableProcedure(table=" + getTableName() + ") id=" + getProcId();
  }

  protected void waitRegionInTransition(final MasterProcedureEnv env,
      final List<HRegionInfo> regions) throws IOException, CoordinatedStateException {
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    RegionStates states = am.getRegionStates();
    long waitTime = env.getMasterServices().getConfiguration()
            .getLong("hbase.master.wait.on.region", 5 * 60 * 1000);
    long waitingTimeForEvents = env.getMasterServices().getConfiguration()
            .getInt("hbase.master.event.waiting.time", 1000);
    for (HRegionInfo region : regions) {
      long done = System.currentTimeMillis() + waitTime;
      while (System.currentTimeMillis() < done) {
        if (states.isRegionInState(region, State.FAILED_OPEN)) {
          am.regionOffline(region);
        }
        if (!states.isRegionInTransition(region)) break;
        try {
          Thread.sleep(waitingTimeForEvents);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while sleeping");
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        LOG.debug("Waiting on region to clear regions in transition; "
          + am.getRegionStates().getRegionTransitionState(region));
      }
      if (states.isRegionInTransition(region)) {
        throw new IOException("Waited hbase.master.wait.on.region (" +
          waitTime + "ms) for region to leave region " +
          region.getRegionNameAsString() + " in transitions");
      }
    }
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.DeleteTableState.Builder state =
      MasterProcedureProtos.DeleteTableState.newBuilder()
        .setTableName(ProtobufUtil.toProtoTableName(tableName));
    if (regions != null) {
      for (HRegionInfo hri: regions) {
        state.addRegionInfo(HRegionInfo.convert(hri));
      }
    }
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.DeleteTableState state =
      MasterProcedureProtos.DeleteTableState.parseDelimitedFrom(stream);
    tableName = ProtobufUtil.toTableName(state.getTableName());
    if (state.getRegionInfoCount() == 0) {
      regions = null;
    } else {
      regions = new ArrayList<HRegionInfo>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri: state.getRegionInfoList()) {
        regions.add(HRegionInfo.convert(hri));
      }
    }
  }

  private void preDelete(final MasterProcedureEnv env) throws IOException {
    if (!MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), tableName)) {
      throw new TableNotFoundException(tableName);
    }

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preDeleteTableHandler(tableName);
    }
  }

  private void postDelete(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postDeleteTableHandler(tableName);
    }

    env.deleteTable(tableName);
  }

  protected static void deleteFromFs(final MasterProcedureEnv env,
      final TableName tableName, final List<HRegionInfo> regions,
      final boolean archive) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final FileSystem fs = mfs.getFileSystem();
    final Path tempdir = mfs.getTempDir();

    final Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), tableName);
    final Path tempTableDir = FSUtils.getTableDir(tempdir, tableName);

    if (fs.exists(tableDir)) {
      // Ensure temp exists
      if (!fs.exists(tempdir) && !fs.mkdirs(tempdir)) {
        throw new IOException("HBase temp directory '" + tempdir + "' creation failure.");
      }

      // Move the table in /hbase/.tmp
      if (!fs.rename(tableDir, tempTableDir)) {
        throw new IOException("Unable to move '" + tableDir + "' to temp '" + tempTableDir + "'");
      }
    }

    // Archive regions from FS (temp directory)
    if (archive) {
      for (HRegionInfo hri : regions) {
        LOG.debug("Archiving region " + hri.getRegionNameAsString() + " from FS");
        HFileArchiver.archiveRegion(fs, mfs.getRootDir(),
            tempTableDir, HRegion.getRegionDir(tempTableDir, hri.getEncodedName()));
      }
      LOG.debug("Table '" + tableName + "' archived!");
    }

    // Delete table directory from FS (temp directory)
    if (!fs.delete(tempTableDir, true)) {
      throw new IOException("Couldn't delete " + tempTableDir);
    }
  }

  protected static List<HRegionInfo> getRegionsFromMeta(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      return new MetaTableLocator().getMetaRegions(env.getMasterServices().getZooKeeper());
    } else {
      return MetaTableAccessor.getTableRegions(env.getMasterServices().getConnection(), tableName);
    }
  }

  /**
   * There may be items for this table still up in hbase:meta in the case where the
   * info:regioninfo column was empty because of some write error. Remove ALL rows from hbase:meta
   * that have to do with this table. See HBASE-12980.
   * @throws IOException
   */
  private static void cleanAnyRemainingRows(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    Scan tableScan = MetaTableAccessor.getScanForTableName(tableName);
    try (Table metaTable =
        env.getMasterServices().getConnection().getTable(TableName.META_TABLE_NAME)) {
      List<Delete> deletes = new ArrayList<Delete>();
      try (ResultScanner resScanner = metaTable.getScanner(tableScan)) {
        for (Result result : resScanner) {
          deletes.add(new Delete(result.getRow()));
        }
      }
      if (!deletes.isEmpty()) {
        LOG.warn("Deleting some vestigal " + deletes.size() + " rows of " + tableName +
          " from " + TableName.META_TABLE_NAME);
        metaTable.delete(deletes);
      }
    }
  }

  protected static void deleteFromMeta(final MasterProcedureEnv env,
      final TableName tableName, List<HRegionInfo> regions) throws IOException {
    LOG.debug("Deleting regions from META");
    MetaTableAccessor.deleteRegions(env.getMasterServices().getConnection(), regions);
  }

  protected static void deleteAssignmentState(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    AssignmentManager am = env.getMasterServices().getAssignmentManager();

    // Clean up regions of the table in RegionStates.
    LOG.debug("Removing '" + tableName + "' from region states.");
    am.getRegionStates().tableDeleted(tableName);

    // If entry for this table states, remove it.
    LOG.debug("Marking '" + tableName + "' as deleted.");
    am.getTableStateManager().setDeletedTable(tableName);

    // Clean any remaining rows for this table.
    cleanAnyRemainingRows(env, tableName);
  }

  protected static void deleteTableDescriptorCache(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    LOG.debug("Removing '" + tableName + "' descriptor.");
    env.getMasterServices().getTableDescriptors().remove(tableName);
  }
}
