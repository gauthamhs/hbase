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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.handler.TableEventHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.server.Finish;
import org.apache.hadoop.hbase.server.Finishable;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.snapshot.SnapshotLogUtils;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

/**
 * General snapshot handler for the master.
 */
@InterfaceAudience.Private
public abstract class TableSnapshotHandler extends TableEventHandler implements Finishable,
    ExceptionCheckable<HBaseSnapshotException> {

  private static final Log LOG = LogFactory.getLog(TableSnapshotHandler.class);

  private final Finish finished = new Finish();
  protected final SnapshotDescription snapshot;

  protected final Configuration conf;
  protected final FileSystem fs;
  protected final Path rootDir;
  protected final SnapshotErrorListener monitor;
  private final SnapshotManager manager;
  private final Path snapshotDir;
  private final Path workingDir;

  /**
   * @param snapshot descriptor of the snapshot to take
   * @param server parent server
   * @param masterServices master services provider
   * @param errorMonitor monitor the health of the snapshot
   * @param manager
   * @throws IOException on unexpected error
   */
  public TableSnapshotHandler(SnapshotDescription snapshot, Server server,
      final MasterServices masterServices, SnapshotErrorListener errorMonitor,
      SnapshotManager manager) throws IOException {
    super(EventType.C_M_SNAPSHOT_TABLE, Bytes.toBytes(snapshot.getTable()), server, masterServices);
    // The next call fails if no such table.
    getTableDescriptor();
    this.snapshot = snapshot;
    this.monitor = errorMonitor;
    this.conf = this.masterServices.getConfiguration();
    this.fs = this.masterServices.getMasterFileSystem().getFileSystem();
    this.rootDir = FSUtils.getRootDir(this.conf);
    this.manager = manager;
    this.snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    this.workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
  }

  @Override
  public void process() {
    LOG.info("Running table snapshot operation " + eventType + " on table " + tableNameStr);
    try {
      List<Pair<HRegionInfo, ServerName>> regions = MetaReader.getTableRegionsAndLocations(
        this.server.getCatalogTracker(), tableName, true);
      // run the snapshot
      snapshot(regions);

      // verify the snapshot is valid
      verifySnapshot(this.workingDir);

      // complete the snapshot
      manager.completeSnapshot(this.snapshotDir, this.workingDir, this.fs);
    } catch (Exception e) {
      monitor.snapshotFailure("Failed due to exception:" + e.getMessage(), snapshot, e);
    } finally {
      LOG.debug("Launching cleanup of working dir:" + workingDir);
      SnapshotCleaner.launchSnapshotCleanup(workingDir, this.fs, this.conf);
    }
  }

  /**
   * Verify that the snapshot in the directory is a valid snapshot
   * @param snapshotDir snapshot directory to check
   */
  private void verifySnapshot(Path snapshotDir) throws CorruptedSnapshotException, IOException {
    // verify snapshot info matches
    verifySnapshotDescription(snapshotDir);

    // check that tableinfo is a valid table description
    verifyTableInfo(snapshotDir);

    // check that each region is valid
    verifyRegionsInfo(snapshotDir);

    // check that the hlogs, if they exist, are valid
    verifyLogs(snapshotDir);
  }

  /**
   * Check that the snapshot description written in the filesystem matches the current snapshot
   * @param snapshotDir snapshot directory to check
   */
  private void verifySnapshotDescription(Path snapshotDir) throws CorruptedSnapshotException {
    SnapshotDescription found = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    if (!this.snapshot.equals(found)) {
      throw new CorruptedSnapshotException("Snapshot read (" + found
          + ") doesn't equal snapshot we ran (" + snapshot + ").");
    }
  }

  /**
   * Check that the table descriptor for the snapshot is a valid table descriptor
   * @param snapshotDir snapshot directory to check
   */
  private void verifyTableInfo(Path snapshotDir) throws IOException {
    FSTableDescriptors.getTableDescriptor(fs, snapshotDir);
  }

  /**
   * Check that all the regions in the the snapshot are valid
   * @param snapshotDir snapshot directory to check
   * @throws IOException if we can't reach .META. or read the files from the FS
   */
  private void verifyRegionsInfo(Path snapshotDir) throws IOException {
    List<HRegionInfo> regions = MetaReader.getTableRegions(this.server.getCatalogTracker(),
      tableName);
    for (HRegionInfo region : regions) {
      verifyRegion(fs, snapshotDir, region);
    }
  }

  /**
   * Verify that the region (regioninfo, hfiles) are valid
   * @param snapshotDir snapshot directory to check
   * @param region the region to check
   */
  private void verifyRegion(FileSystem fs, Path snapshotDir, HRegionInfo region) throws IOException {
    // make sure we have region in the snapshot
    Path regionDir = new Path(snapshotDir, region.getEncodedName());
    if (!fs.exists(regionDir)) {
      throw new CorruptedSnapshotException("No region directory found for region:" + region);
    }
    // make sure we have the region info in the snapshot
    Path regionInfo = new Path(regionDir, HRegion.REGIONINFO_FILE);
    // make sure the file exists
    if (!fs.exists(regionInfo)) {
      throw new CorruptedSnapshotException("No region info found for region:" + region);
    }
    FSDataInputStream in = fs.open(regionInfo);
    HRegionInfo found = HRegionInfo.parseFrom(in);
    if (!region.equals(found)) {
      throw new CorruptedSnapshotException("Found region info (" + found
          + ") doesn't match know region:" + region);
    }

    // check for the existance of each hfile
    PathFilter dir = new FSUtils.VisibleDirectory(fs);
    FileStatus[] columnFamilies = FSUtils.listStatus(fs, regionDir, dir);
    // should we do some checking here to make sure the cfs are correct?
    if (columnFamilies == null || columnFamilies.length == 0) return;

    // setup the postfixes
    Path tablePostFix = new Path(tableNameStr);
    Path regionPostFix = new Path(tablePostFix, region.getEncodedName());

    // get the potential real paths
    Path archivedRegion = new Path(HFileArchiveUtil.getArchivePath(conf), regionPostFix);
    Path realRegion = new Path(rootDir, regionPostFix);

    // loop through each cf and check we can find each of the hfiles
    for (FileStatus cf : columnFamilies) {
      FileStatus[] hfiles = FSUtils.listStatus(fs, cf.getPath(), null);
      // should we check if there should be hfiles?
      if (hfiles == null || hfiles.length == 0) continue;

      Path realCfDir = new Path(realRegion, cf.getPath().getName());
      Path archivedCfDir = new Path(archivedRegion, cf.getPath().getName());
      for (FileStatus hfile : hfiles) {
        // make sure the name is correct
        if (!StoreFile.validateStoreFileName(hfile.getPath().getName())) {
          throw new CorruptedSnapshotException("HFile: " + hfile.getPath()
              + " is not a valid hfile name.");
        }

        // check that we can read in the reference
        Reference.read(fs, hfile.getPath());

        // check to see if hfile is present in the real table
        String fileName = Reference.getDeferencedHFileName(hfile.getPath().getName());
        Path file = new Path(realCfDir, fileName);
        Path archived = new Path(archivedCfDir, fileName);
        if (!fs.exists(file) && !fs.equals(archived)) {
          throw new CorruptedSnapshotException("Cann't find hfile: " + hfile.getPath()
              + " in the real (" + archivedCfDir + ") or archive (" + archivedCfDir
              + ") directory for the primary table.");
        }
      }
    }
  }

  /**
   * Check that the logs stored in the log directory for the snapshot is valid. If no log directory
   * is present, validation is skipped.
   * @param snapshotDir snapshot directory to check
   * @throws CorruptedSnapshotException if the hlogs in the snapshot are broken
   * @throws IOException if we can't reach the filesystem
   */
  private void verifyLogs(Path snapshotDir) throws CorruptedSnapshotException, IOException {
    List<FileStatus> logs = SnapshotLogUtils.getSnapshotHLogs(fs, snapshotDir);
    // if we don't have any logs, we are done
    if (logs.size() == 0) {
      LOG.info("No HLogs found, skipping verification.");
      return;
    }

    // XXX - this is just for a regular table - snapshotting META/ROOT with a globally consistent
    // snapshot because their log directories are structured differently than a regular table
    Path realLogs = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldlogs = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    for (FileStatus log : logs) {

      // find the real directory
      Path serverDir = log.getPath().getParent();
      Path realLogDir = new Path(realLogs, serverDir.getName());

      // deference the file name so we can lookup the real file
      String fileName = log.getPath().getName();
      // remove the reference file info
      String realFile = fileName.substring(0, fileName.lastIndexOf('.'));
      Path realLog = new Path(realLogDir, realFile);

      if (fs.exists(realLog)) {
        LOG.debug("Log:" + realLog + " exists and is in snapshot via:" + log);
        continue;
      }

      // otherwise, we need to check to it in the oldlogs directory
      String oldLogFilename = SnapshotLogUtils.getOldLogsName(fileName);
      Path oldLogfilePath = new Path(oldlogs, oldLogFilename);
      if (!fs.exists(oldLogfilePath)) {
        throw new CorruptedSnapshotException("Cannot find log: " + log.getPath()
            + " in primary logs dir (" + realLogs + ") or in oldlogs directory (" + oldlogs + ")");
      }
    }
  }

  /**
   * Run a snapshot from the master
   */
  protected abstract void snapshot(List<Pair<HRegionInfo, ServerName>> regions) throws IOException,
      KeeperException;

  @Override
  public final void finish() {
    finished.finish();
  }

  @Override
  public boolean getFinished() {
    return finished.getFinished();
  }

  public SnapshotDescription getSnapshot() {
    return snapshot;
  }

  @Override
  public boolean checkForError() {
    return this.monitor.checkForError();
  }

  @Override
  public void failOnError() throws HBaseSnapshotException {
    this.monitor.failOnError();
  }
}