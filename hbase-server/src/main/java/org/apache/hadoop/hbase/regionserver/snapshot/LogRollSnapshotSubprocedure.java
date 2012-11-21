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
package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.server.snapshot.task.ReferenceServerWALsTask;

/**
 * This online snapshot implementation forces a log roll and uses the distributed procedure
 * framework.  Its enter stage forces an HLog roll and then builds the region servers snapshot
 * manifest from its hfiles, hlogs and copies .regioninfos into the snapshot working directory.
 * The leave stage doesn't really do anything.  At the master side, there is an atomic rename
 * of the working dir into the proper snapshot directory.
 */
public class LogRollSnapshotSubprocedure extends Subprocedure {
  private static final Log LOG = LogFactory.getLog(LogRollSnapshotSubprocedure.class);

  private final List<HRegion> regions;
  private final SnapshotDescription snapshot;
  private final SnapshotSubprocedurePool taskManager;
  private final FileSystem fs;
  private final Configuration conf;

  public LogRollSnapshotSubprocedure(ProcedureMember member,
      ForeignExceptionDispatcher errorListener, long wakeFrequency, long timeout,
      List<HRegion> regions, SnapshotDescription snapshot, Configuration conf,
      SnapshotSubprocedurePool taskManager, FileSystem fs) {
  
    super(member, snapshot.getName(), errorListener, wakeFrequency, timeout);
    this.snapshot = snapshot;
    this.regions = regions;
    this.taskManager = taskManager;
    this.fs = fs;
    this.conf = conf;
  }

  /**
   * Callable for adding files to snapshot manifest working dir.  Ready for multithreading.
   */
  class RegionSnapshotTask implements Callable<Void> {
    HRegion region;
    RegionSnapshotTask(HRegion region) {
      this.region = region;
    }
    
    @Override
    public Void call() throws Exception {
      region.addRegionToSnapshot(snapshot, monitor);
      return null;
    }
    
  }

  /**
   * Tracker that is used to get the new HLog's filename which is used to get the directory
   * that the other hlogs of this region server resides in.
   */
  class LogRollTracker implements WALActionsListener {
    Path newLog = null;
    
    public Path getNewLogPath() {
      return newLog;
    }
    
    @Override
    public void preLogRoll(Path oldPath, Path newPath) throws IOException {
      LOG.debug("preLogRoll: old: " + oldPath + " new: " + newPath);
      // Don't care
    }

    @Override
    public void postLogRoll(Path oldPath, Path newPath) throws IOException {
      LOG.debug("postLogRoll: old: " + oldPath + " new: " + newPath);
      this.newLog = newPath;
    }

    @Override
    public void preLogArchive(Path oldPath, Path newPath) throws IOException {
      // Don't care
      LOG.debug("preLogArchive: old: " + oldPath  + " new: " + newPath);
    }

    @Override
    public void postLogArchive(Path oldPath, Path newPath) throws IOException {
      // Don't care
      LOG.debug("preLogArchive: old: " + oldPath + " new: " + newPath);
    }

    @Override
    public void logRollRequested() {
      // Don't care
      LOG.debug("logRollReequested");
    }

    @Override
    public void logCloseRequested() {
      // Don't care
      LOG.debug("logCloseReequested");
    }

    @Override
    public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
        WALEdit logEdit) {
      // Don't care
    }

    @Override
    public void visitLogEntryBeforeWrite(HTableDescriptor htd,
        HLogKey logKey, WALEdit logEdit) {
      // Don't care
    }
    
  };

  private void logrollSnapshot() throws ForeignException {
    if (regions.size() == 0 ) {
      // No regions on this RS, we are basically done.
      return;
    }

    monitor.rethrowException();

    // Roll the hlog and add log reference.  In this flavor of snapshot, this doesn't necessarily
    // have to happen before the hfiles are snapshotted.

    // TODO what if any locking do we need to do here? 1) prevent node from splitting, closing, region moves

    // TODO: This assumes we have only one log for all the regions (this will potentially change
    // in the future).
    final HLog hlog = regions.get(0).getLog();
    LogRollTracker tracker = new LogRollTracker();
    try {
      hlog.registerWALActionsListener(tracker);
      hlog.rollWriter(true); // force a roll to get log dir, probably don't need this roll
    } catch (FailedLogCloseException e1) {
      throw new ForeignException(e1);
    } catch (IOException e1) {
      throw new ForeignException(e1);
    } finally {
      hlog.unregisterWALActionsListener(tracker);
    }

    // Figure out where references go.
    Path p = tracker.getNewLogPath();
    if (p == null) {
      String msg = "Rolled a log for snapshot '" + snapshot.getName() + "' but didn't get the" +
          " path of the rolled log file!  Bailing out."; 
      LOG.warn(msg);
      throw new ForeignException(msg);
    }

    Path serverLogDir = p.getParent();
    ReferenceServerWALsTask walsTask = new ReferenceServerWALsTask(snapshot, monitor,
        serverLogDir, conf, fs);
//    ReferenceServerWALsTask walsTask = new ReferenceServerWALsTask(snapshot, monitor,
//        serverLogDir, conf, fs) {
//      @Override
//      public Void call() throws IOException, ForeignException {
//        super.call();
//        hlog.rollWriter(true); // close off any new stuff 
//        return null;
//      }
//    };
    taskManager.submitTask(walsTask);
    monitor.rethrowException();
    
    // Add all hfiles already existing in region.  If by chance a flush right after the roll, it
    // is ok to have extra HFiles with data duplicated in HLog.
    for (HRegion region : regions) {
      taskManager.submitTask(new RegionSnapshotTask(region));
      monitor.rethrowException();
    }

    // wait for everything to complete.
    taskManager.waitForOutstandingTasks();
  }

  @Override
  public void acquireBarrier() throws ForeignException {
    // do nothing, executing in inside barrier step.
  }

  /**
   * Force a roll all the current hlog files, and then record the name of all the files and
   * copy regioninfo.
   */
  @Override
  public void insideBarrier() throws ForeignException {
    logrollSnapshot();
  }

  /**
   * Cancel threads if they haven't finished.
   */
  @Override
  public void cleanup(Exception e) {
    taskManager.abort("Aborting all log roll online snapshot subprocedure task threads for '"
        + snapshot.getName() + "' due to error", e);
  }

  /**
   * Hooray!
   */
  public void releaseBarrier() {
    // NO OP
  }

}
