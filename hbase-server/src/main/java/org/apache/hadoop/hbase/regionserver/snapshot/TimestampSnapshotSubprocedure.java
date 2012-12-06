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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionListener;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.snapshot.task.TableInfoCopyTask;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Take a timestamp-consistent snapshot for a set of regions of a table on a regionserver.
 * 
 * For each specified region, this class initiates a "split" memstore, picks a time in the near
 * future, commits buffered writes to the appropriate memstore, waits for time to expire, flushes
 * the older memstore to disk and then finally restores to a single memstore state by evicting
 * the older memstore and promoting the newer memstore.
 */
public class TimestampSnapshotSubprocedure extends Subprocedure {

  private static final Log LOG = LogFactory.getLog(TimestampSnapshotSubprocedure.class);

  // Parameters
  /**
   * This is the timestamp used to pick between memstores when we are in split memstore state.
   */
  private final long millisUntilSplit;
  protected final SnapshotDescription snapshot;
  protected final List<HRegion> regions;

  // Actors
  protected List<Callable<Void>> ops;
  private final RegionSnapshotOperationStatus progressMonitor;

  // References
  protected final SnapshotSubprocedurePool taskManager;
  protected final FileSystem fs;
  protected final Configuration conf;

  /**
   *
   * TODO This is an extra layer of reference passing because of the shared TableSnapshotHandler.
   *
   * @param member
   * @param errorListener
   * @param wakeFrequency Frequency in millis to check for region progress and subproc timeout
   * @param timeout Max time to elapse in millis before aborting subprocedure
   * @param regions list of regions to snapshot
   * @param snapshot snapshot description
   * @param conf hbase configuration
   * @param taskManager pool for per region snapshotting tasks
   * @param monitorFactory factory that creates snapshot error monitors ... for ???
   * @param fs hdfs file system
   * @param res RemoteException Serialization mechanism (has some state)
   */
  public TimestampSnapshotSubprocedure(ProcedureMember member,
      ForeignExceptionDispatcher errorListener,
      long wakeFrequency, long timeout,
      List<HRegion> regions, SnapshotDescription snapshot, Configuration conf,
      SnapshotSubprocedurePool taskManager,
      FileSystem fs) {
    super(member, snapshot.getName(), errorListener, wakeFrequency, timeout);
    this.snapshot = snapshot;
    this.regions = regions;
    this.taskManager = taskManager;
    this.fs = fs;
    this.conf = conf;
    this.millisUntilSplit = calculateSplitPoint();

    // create a progress monitor to keep track of the each region's snapshot progress
    this.progressMonitor = new RegionSnapshotOperationStatus(regions.size(), wakeFrequency);

    // Any external exceptions sent here will get converted
    errorListener.addErrorListener(new ForeignExceptionListener() {
      @Override
      public void receiveError(String message, ForeignException e, Object... info) {
          cancel(message, e);
      }
    });
  }

  /**
   * Default for snapshotCreationTime is 0.
   *
   * TODO why is this the algorithm?  What is the meaning of creationTime?
   *
   * @return timestamp for the amount of time to wait before memstore flushing.
   */
  long calculateSplitPoint() {
    long time = snapshot.getCreationTime() - EnvironmentEdgeManager.currentTimeMillis();
    if (time <= 0) {
      LOG.debug("Split duration <= 0, flushing snapshot immediately.");
      time = 0;
    }
    return time;
  }

  /**
   * An adaptor
   *
   * TODO get rid of this extra layer.
   *
   * {@link ExceptionVisitor} for overall operation
   * {@link ProcedureErrorListener}. Any failure in any of the sub-tasks
   * (table-info copy, hfile referencing etc.) is propagated as a local error back to this member's
   * error listener, which then ensures that failure is propagated back up the coordinator.
   */

  /**
   * Submits a task that copies .tableinfo from source table to snapshot
   * @throws IOException
   */
  protected final void submitTableInfoCopy() throws IOException {
    taskManager.submitTask(new TableInfoCopyTask(monitor, snapshot, fs, FSUtils
        .getRootDir(this.conf)));
  }

  /**
   * This kicks off a task per target region and one for copying .tableinfo.  Returns after all
   * these tasks are completed, or throws a procedure exception if a failure occurs in any task
   * or an external abort cancels the operation.
   */
  @Override
  public void acquireBarrier() throws ForeignException {
    try {
      // 1. create a flush operation for each region
      this.ops = new ArrayList<Callable<Void>>(regions.size());
      for (HRegion region : regions) {
        ops.add(new FlushRegionAtTimestampTask(snapshot, region, monitor,
            progressMonitor, wakeFrequency, millisUntilSplit));
      }

      // 2. submit those operations to the region snapshot runner
      for (final Callable<Void> op : ops) {
        taskManager.submitTask(op);
      }

      // 3. do the tableinfo copy async
      monitor.rethrowException();
      submitTableInfoCopy();  // TODO This is goofy -- each region copies to tableinfo?

      // 4. Wait for the regions to complete their snapshotting or an error
      this.taskManager.waitForOutstandingTasks();
    } catch (ForeignException e) {
      throw e;
    } catch (IOException e) {
      // TODO: Why do we wrap this
      throw new ForeignException(e, snapshot.toByteArray());
    } catch (Exception e) {
      // TODO
      LOG.error("This is completely unexpected");
      throw new ForeignException(e, snapshot.toByteArray());
    } finally {
      LOG.debug("Done preparing timestamp request.");
    }
  }

  /**
   * This is essentially a noop.
   *
   * TODO (Does this block other snapshots from occuring.)
   */
  @Override
  public void insideBarrier() throws ForeignException {
    // TODO what are we checking here?

    // wait for the snapshot to complete on all the regions
    LOG.debug("Waiting for operations to complete.");
    // if false, we had an error so we throw an exception.
    if (!progressMonitor.waitUntilDone(monitor)) {
      throw new ForeignException(new HBaseSnapshotException(
      "Found an error while waiting for snapshot '" + snapshot.getName()
      + "' to complete, quiting!"), snapshot.toByteArray());
    }
  }

  /**
   * This is essentially a no-op
   */
  @Override
  public void cleanup(Exception e) {
    LOG.debug("Cleanup snapshot - handled in sub-tasks on error");
  }
}
