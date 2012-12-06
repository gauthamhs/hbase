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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreOperator;
import org.apache.hadoop.hbase.regionserver.SwapStores.SwapForNonSnapshotStores;
import org.apache.hadoop.hbase.regionserver.TimePartitionedStore;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Take a snapshot of the specified region using the time stamp as the basis for consistency.
 *
 * If the task encounters an error register the error with the subprocedure's error listener.
 *
 * NOTE: This used to be a "procedure" but now has been converted to a plain Runnable.  No need
 * for the extra overhead.
 */
public class FlushRegionAtTimestampTask implements Callable<Void> {
  private static final Log LOG = LogFactory.getLog(FlushRegionAtTimestampTask.class);

  // Parameters
  private final long splitPoint; // ts that decides into which store writes proceed
  protected final long wakeFrequency;
  private final HRegion region;
  private final SnapshotDescription snapshot;

  // State
  private Pair<Long, Long> idAndStart; // <logseqId, starttime> for region
  private final CountDownLatch swappedStores = new CountDownLatch(1);

  // References
  private final ForeignExceptionDispatcher remoteErrorSentinel;
  protected final RegionSnapshotOperationStatus monitor;

  // Internals
  private final Timer timer; //  used for scheduling flush/swap
  private final MonitoredTask status; // for external hbase ui logging.

  /**
   * @param snapshot The containing snapshot this is part of
   * @param region Region to snapshot (methods are on HRegion instance)
   * @param remoteErrorSentinel periodically check for if there are remote errors.
   * @param progressMonitor essentially a latch for all regions on the current RS
   * @param wakeFrequency millis between doneness checks
   * @param splitPoint timestamp in millis to split the memstore at
   */
  public FlushRegionAtTimestampTask(SnapshotDescription snapshot, HRegion region,
      ForeignExceptionDispatcher remoteErrorSentinel,
      RegionSnapshotOperationStatus progressMonitor,
      long wakeFrequency, long splitPoint) {
    this.region = region;
    this.snapshot = snapshot;
    this.splitPoint = splitPoint;
    this.remoteErrorSentinel = remoteErrorSentinel;
    this.wakeFrequency = wakeFrequency;
    this.monitor = progressMonitor;
    timer = new Timer("memstore-snapshot-flusher-timer", true);
    status = TaskMonitor.get().createStatus("Starting timestamp-consistent snapshot" + snapshot);
  }

  @Override
  public Void call() throws Exception {
    // run the operation
    try {
      // start by checking for external error first
      remoteErrorSentinel.rethrowException();
      prepRegion();

      // wait for the commit allowed latch to release
      remoteErrorSentinel.rethrowException(); // if Coordinator aborts, will bail from here with exception
      completeSnapshot();

      remoteErrorSentinel.rethrowException();
    } catch (Exception e) {
      LOG.error("Flush region at timestamp for region " + region + " failed!", e);

      // Cleanup is handled by the coordinator side.  Throw exn
      throw e;

    } finally {
      LOG.debug("Running finish phase.");
      restoreRegion();
    }
    return null;
  }

  /**
   * Start a timestamp consistent snapshot operation, and then wait until swap time has triggered.
   */
  public void prepRegion() throws SnapshotCreationException {
    // flush the snapshot in the future
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        // wait for the stores to be swapped before committing
        try {
          ForeignExceptionDispatcher.waitForLatch(swappedStores, FlushRegionAtTimestampTask.this.remoteErrorSentinel,
              wakeFrequency, "snapshot store-swap");
        } catch (ForeignException e) {
          // ignore - this will be picked up by the main watcher
          return;
        } catch (InterruptedException e) {
          // ignore - this will be picked up by the main watcher
          return;
        } catch (Exception e) {
          // ignore - this will be picked up by the main watcher
          return;
        }
      }
    };
    LOG.debug("Current split point:" + splitPoint);
    long splitDuration = splitPoint - EnvironmentEdgeManager.currentTimeMillis();
    if (splitDuration < 0) splitDuration = 0;
    LOG.debug("Scheduling snapshot commit in " + splitDuration + " ms");
    timer.schedule(task, splitDuration);

    // prepare for the snapshot
    try {
      LOG.debug("Starting snasphot on region " + this.region);
      idAndStart = this.region.startTimestampConsistentSnapshot(this.snapshot, splitPoint,
        splitDuration, remoteErrorSentinel, status);
      // notify that we have swapped the stores and are ready for commit
      swappedStores.countDown();
    } catch (IOException e) {
      throw new SnapshotCreationException("IOExcpetion when starting", e, this.snapshot);
    }
  }

  /**
   * "complete" timestamp consistent snapshot by flushing.  Then snapshot data
   * to add data to file system snapshot manifest.
   * @throws ForeignException
   */
  public final void completeSnapshot() throws SnapshotCreationException {
    // now write the in-memory state to disk for the snapshot
    // since we don't release the snapshot-write-lock, we don't get a compaction
    // and don't need to worry about on-disk state getting munged too much
    try {
      LOG.debug("Starting commit of timestamp snapshot.");
      this.region.completeTimestampConsistentSnapshot(this.snapshot, this.idAndStart.getFirst(),
        this.idAndStart.getSecond(), this.monitor, this.remoteErrorSentinel, this.status);
    } catch (ClassCastException e1) {
      // happens if the stores got swapped back prematurely
      throw new SnapshotCreationException("ClassCastException on complete", e1, this.snapshot);
    } catch (IOException e2) {
      throw new SnapshotCreationException("IOException on complete", e2, this.snapshot);
    }
    // write the committed files for the snapshot
    LOG.debug(this.region + " is committing files for snapshot.");
    try {
      this.region.addRegionToSnapshot(this.snapshot, remoteErrorSentinel);
    } catch (IOException e) {
      throw new SnapshotCreationException("Couldn't complete snapshot", e, this.snapshot);
    }
  }

  /**
   * Verify that regions are ok after the swap and memstore.
   */
  public final void restoreRegion() {
    LOG.debug("Checking to see if we need to swap snapshot stores");
    CheckForSwappedStores check = new CheckForSwappedStores(this.region);
    SwapForNonSnapshotStores operator = new SwapForNonSnapshotStores(this.region);
    try {
      this.region.operateOnStores(check);
      if (check.foundSwappedStores) {
        LOG.debug("Found stores that have been swapped for snapshot stores, so swapping them back.");
        this.region.operateOnStores(operator);
      }
    } catch (IOException e) {
      LOG.error("Failed to swap back stores!", e);
      throw new RuntimeException("Failed to swap back generic stores!", e);
    }

    LOG.debug("finishing timestamp snapshot.");
    this.region.restoreRegionAfterTimestampSnapshot();
    this.status.markComplete("Completed timestamp-based snapshot (" + this.snapshot + ").");
    this.monitor.getFinishLatch().countDown();
  }
  
  public class CheckForSwappedStores extends StoreOperator<IOException> {
    public boolean foundSwappedStores = false;

    public CheckForSwappedStores(HRegion parent) {
      super(parent);
    }

    @Override
    public void prepare() {
      // NOOP
    }

    @Override
    public void operateOnStore(Store store) throws IOException {
      // if we didn't find a swapped store, then check if it swapped.
      if (!foundSwappedStores && store instanceof TimePartitionedStore) foundSwappedStores = true;
    }

    @Override
    public void finish() {
      // NOOP
    }
  }

}