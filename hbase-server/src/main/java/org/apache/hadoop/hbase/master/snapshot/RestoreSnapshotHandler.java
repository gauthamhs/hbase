/**
 *
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

package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.SnapshotHandler;
import org.apache.hadoop.hbase.master.handler.TableEventHandler;
import org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.restore.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSDiskOperationLock;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to Restore a snapshot.
 */
@InterfaceAudience.Private
public class RestoreSnapshotHandler extends TableEventHandler implements SnapshotHandler {
  private static final Log LOG = LogFactory.getLog(RestoreSnapshotHandler.class);

  private final HTableDescriptor hTableDescriptor;
  private final SnapshotDescription snapshot;

  private final SnapshotExceptionSnare monitor;
  private final OperationAttemptTimer timer;

  private volatile boolean finished = false;
  private volatile boolean stopped = false;

  public RestoreSnapshotHandler(final MasterServices masterServices,
      final SnapshotDescription snapshot, final HTableDescriptor htd, long waitTime)
      throws IOException {
    super(EventType.C_M_RESTORE_SNAPSHOT, htd.getName(), masterServices, masterServices);

    // Snapshot information
    this.snapshot = snapshot;

    // Monitor
    this.monitor = new SnapshotExceptionSnare(snapshot);
    this.timer = new OperationAttemptTimer(monitor, waitTime, snapshot);

    // Check table exists.
    getTableDescriptor();

    // This is the new schema we are going to write out as this modification.
    this.hTableDescriptor = htd;
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> hris) throws IOException {
    MasterFileSystem fileSystemManager = masterServices.getMasterFileSystem();
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    Path tableDir = HTableDescriptor.getTableDir(rootDir, tableName);
    Path lockFile = FSDiskOperationLock.getTableOperationLockFile(tableDir);

    try {
      timer.start();

      FSDiskOperationLock oplock = new FSDiskOperationLock(
        FSDiskOperationLock.Type.RESTORE_TABLE, snapshot.getName());
      oplock.write(fs, lockFile);

      // Update descriptor
      this.masterServices.getTableDescriptors().add(hTableDescriptor);

      Configuration conf = masterServices.getConfiguration();
      CatalogTracker catalogTracker = masterServices.getCatalogTracker();
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);

      byte[] tableName = hTableDescriptor.getName();

      // Execute the Restore
      LOG.debug("Starting restore snapshot=" + snapshot);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs,
          catalogTracker, snapshot, snapshotDir, hTableDescriptor, tableDir, monitor);
      restoreHelper.restore();

      // At this point the restore is complete. Next step is enabling the table.
      LOG.info("Restore snapshot=" + snapshot.getName() + " on table=" +
        Bytes.toString(tableName) + " completed!");
      timer.complete();

      hris.clear();
      hris.addAll(MetaReader.getTableRegions(catalogTracker, tableName));
    } catch (IOException e) {
      String msg = "restore snapshot=" + snapshot + " failed";
      LOG.error(msg, e);
      monitor.snapshotFailure("Failed due to exception:" + e.getMessage(), snapshot, e);
      throw new RestoreSnapshotException(msg, e);
    } finally {
      this.finished = true;
      fs.delete(lockFile, false);
    }
  }

  @Override
  public boolean isFinished() {
    return this.finished;
  }

  @Override
  public SnapshotDescription getSnapshot() {
    return snapshot;
  }

  @Override
  public void stop(String why) {
    if (this.stopped) return;
    this.stopped = true;
    LOG.info("Stopping restore snapshot=" + snapshot + " because: " + why);
    this.monitor.snapshotFailure("Failing restore because server is stopping.", snapshot);
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public HBaseSnapshotException getExceptionIfFailed() {
    try {
      this.monitor.failOnError();
    } catch (HBaseSnapshotException e) {
      return e;
    }
    return null;
  }
}
