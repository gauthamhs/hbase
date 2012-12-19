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

import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionCheckable;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;

/**
 * A latch that is passed to each region thread so that notification can be passed when all
 * selected regions on a region server have been snapshotted.
 */
public class RegionSnapshotOperationStatus {

  private static final Log LOG = LogFactory.getLog(RegionSnapshotOperationStatus.class);
  private long wakeFrequency;
  private final CountDownLatch done;

  // per region stability info
  protected int totalRegions = 0;

  /**
   * Monitor the status of the operation.
   * @param regionCount number of regions to complete the operation before the operation is ready.
   * @param wakeFrequency frequency to check for errors while waiting for regions to prepare
   */
  public RegionSnapshotOperationStatus(int regionCount, long wakeFrequency) {
    this.wakeFrequency = wakeFrequency;
    this.totalRegions = regionCount;
    this.done = new CountDownLatch(regionCount);
  }

  public CountDownLatch getFinishLatch() {
    return this.done;
  }

  /**
   * Each Region snapshotting task is passed an instance of this object and can wait until the monitor has completed.
   *
   * TODO this wasn't documented, not sure what intent was.
   * @param failureMonitor monitor to check periodically for errors
   * @return <tt>true</tt> on success, <tt>false</tt> if an error is found
   */
  public boolean waitUntilDone(ForeignExceptionDispatcher failureMonitor) {
    LOG.debug("Expecting " + totalRegions + " to complete snapshot.");
    return waitOnCondition(done, failureMonitor, "regions to complete snapshot");
  }

  /**
   * @return true if no failures, false if failure found.
   */
  protected boolean waitOnCondition(CountDownLatch latch,
      ForeignExceptionCheckable failureMonitor, String info) {
    try {
      ForeignExceptionDispatcher.waitForLatch(latch, failureMonitor, wakeFrequency, info);
    } catch (ForeignException e) {
      LOG.warn("Error found while :" + info, e);
      return false;
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for :" + info);
      return false;
    }
    // if there was an error, then we can't claim success
    return !failureMonitor.hasException();
  }
}