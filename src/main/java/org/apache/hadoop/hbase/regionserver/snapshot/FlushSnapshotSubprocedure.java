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

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * This online snapshot implementation forces a logflush and uses the distributed procedure
 * framework.  Its enter stage forces does nothing.  Its leave stage then builds the region
 * server's snapshot manifest from its hfiles and copies .regioninfos into the snapshot working
 * directory.  At the master side, there is an atomic rename of the working dir into the proper
 * snapshot directory.
 */
public class FlushSnapshotSubprocedure extends Subprocedure {
  private static final Log LOG = LogFactory.getLog(FlushSnapshotSubprocedure.class);

  private final List<HRegion> regions;
  private final SnapshotDescription snapshot;
  private final SnapshotSubprocedurePool taskManager;

  public FlushSnapshotSubprocedure(ProcedureMember member,
      ForeignExceptionDispatcher errorListener, long wakeFrequency, long timeout,
      List<HRegion> regions, SnapshotDescription snapshot,
      SnapshotSubprocedurePool taskManager) {
  
    super(member, snapshot.getName(), errorListener, wakeFrequency, timeout);
    this.snapshot = snapshot;
    this.regions = regions;
    this.taskManager = taskManager;
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
      LOG.info("Flush Snapshotting region " + region.toString());
      region.flushcache();
      region.addRegionToSnapshot(snapshot, monitor);
      return null;
    }
    
  }

  private void flushSnapshot() throws ForeignException {
    if (regions.size() == 0 ) {
      // No regions on this RS, we are basically done.
      return;
    }

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
   * do a flush snapshot of every region on this rs from the target table.
   */
  @Override
  public void insideBarrier() throws ForeignException {
    flushSnapshot();
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
