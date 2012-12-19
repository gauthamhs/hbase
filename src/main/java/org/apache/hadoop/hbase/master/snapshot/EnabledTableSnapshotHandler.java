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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager;
import org.apache.hadoop.hbase.procedure.Procedure;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;

/**
 * Handle the snapshot of an online table, regardless of snapshot table. Uses a
 * {@link Procedure} to run the snapshot across all the involved regions.
 * @see ProcedureCoordinator
 */
@InterfaceAudience.Private
public class EnabledTableSnapshotHandler extends TakeSnapshotHandler {

  private static final Log LOG = LogFactory.getLog(EnabledTableSnapshotHandler.class);
  private Procedure proc = null;
  private ProcedureCoordinator coordinator;

  // MASTER Side
  public EnabledTableSnapshotHandler(SnapshotDescription snapshot, MasterServices master,
      SnapshotManager manager, long wakeFrequency) throws IOException {
    super(snapshot, master);
    // set the current status
    
    this.coordinator = manager.getCoordinator();
  }

  // TODO consider switching over to using regionnames, rather than server names. This would allow
  // regions to migrate during a snapshot, and then be involved when they are ready. Still want to
  // enforce a snapshot time constraints, but lets us to potentially be a bit more robust.

  /**
   * This method kicks off a snapshot procedure.  Other than that it hangs around for various
   * phases to complete.
   *
   * TODO verify that regions is locked an unchangeable while this is going on.
   */
  @Override
  protected void snapshotRegions(List<Pair<HRegionInfo, ServerName>> regions)
      throws HBaseSnapshotException {
    Set<String> regionServers = new HashSet<String>(regions.size());
    for (Pair<HRegionInfo, ServerName> region : regions) {
      regionServers.add(region.getSecond().toString());
    }

    // start the snapshot on the RS
    proc = coordinator.startProcedure(this.snapshot.getName(),
      this.snapshot.toByteArray(), Lists.newArrayList(regionServers));
    if (proc == null) {
      String msg = "Failed to submit distribute procedure for snapshot '" + snapshot.getName() + "'";
      LOG.error(msg);
      throw new HBaseSnapshotException(msg);
    }

    try {
      // wait for the snapshot to complete
      proc.waitForCompleted();
      LOG.info("Done waiting - snapshot finished!");

      // TODO verify that something will eventually come and interrupt this thread.  If not this
      // could consume all threads in the Master's executor pool.
    } catch (InterruptedException e) {
      ForeignException ee = new ForeignException("Interrupted while waiting for snapshot to finish", e);
      getMonitor().receiveError("Interrupted while waiting for snapshot to finish", ee, getSnapshot());
      Thread.currentThread().interrupt();
    } catch (ForeignException e) {
      ForeignException ee = new ForeignException("Failure while waiting for snapshot to finish", e);
      getMonitor().receiveError("Failure while waiting for snapshot to finish", ee, getSnapshot());
    }
  }

  public Procedure getProcedure() {
    return proc;
  }

  @Override
  protected ForeignExceptionDispatcher getMonitor() {
    if (proc == null) {
      return null;
    }
    return proc.getErrorMonitor();
  }
}