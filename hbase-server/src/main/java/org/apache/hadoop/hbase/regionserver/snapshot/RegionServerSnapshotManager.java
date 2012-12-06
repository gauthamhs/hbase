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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.snapshot.MasterSnapshotVerifier;
import org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.procedure.SubprocedureFactory;
import org.apache.hadoop.hbase.procedure.ZKProcedureMemberRpcs;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This manager class handles the work dealing with snapshots for a {@link HRegionServer}.
 * <p>
 * This provides the mechanism necessary to kick off a online snapshot specific
 * {@link Subprocedure} that is responsible for the regions being served by this region server.
 * If any failures occur with the subprocedure, the manager's procedure member notifies the
 * procedure coordinator to abort all others. 
 * <p>
 * On startup, requires {@link #start()} to be called.
 * <p>
 * On shutdown, requires {@link #close()} to be called
 */
public class RegionServerSnapshotManager extends Configured implements Abortable, Closeable {

  private static final Log LOG = LogFactory.getLog(RegionServerSnapshotManager.class);

  /** Conf key for number of request threads to start snapshots on regionservers */
  public static final String SNAPSHOT_REQUEST_THREADS_KEY = "hbase.snapshot.region.pool.threads";
  /** # of threads for snapshotting regions on the rs. */
  public static final int SNAPSHOT_REQUEST_THREADS_DEFAULT = 10;

  /** Conf key for max time to keep threads in snapshot request pool waiting */
  public static final String SNAPSHOT_TIMEOUT_MILLIS_KEY = "hbase.snapshot.region.timeout";
  /** Keep threads alive in request pool for max of 60 seconds */
  public static final long SNAPSHOT_TIMEOUT_MILLIS_DEFAULT = 60000;

  /** Conf key for millis between checks to see if snapshot completed or if there are errors*/
  public static final String SNAPSHOT_REQUEST_WAKE_MILLIS_KEY = "hbase.snapshot.region.wakefrequency";
  /** Default amount of time to check for errors while regions finish snapshotting */
  private static final long SNAPSHOT_REQUEST_WAKE_MILLIS_DEFAULT = 500;

  private final RegionServerServices rss;
  private final ProcedureMemberRpcs memberRpcs;
  private final ProcedureMember member;
  private volatile boolean aborted;
  private final long wakeMillis;
  private final long timeoutMillis;
  private final SnapshotSubprocedurePool taskManager;

  /**
   * Exposed for testing.
   * @param conf
   * @param parent parent running the snapshot handler
   * @param controller use a custom snapshot controller
   * @param cohortMember use a custom cohort member
   */
   RegionServerSnapshotManager(Configuration conf, HRegionServer parent,
      ProcedureMemberRpcs controller,
      ProcedureMember cohortMember) {
    super(conf);
    this.rss = parent;
    this.memberRpcs = controller;
    this.member = cohortMember;
    // read in the snapshot request configuration properties
    wakeMillis = conf.getLong(SNAPSHOT_REQUEST_WAKE_MILLIS_KEY, SNAPSHOT_REQUEST_WAKE_MILLIS_DEFAULT);
    timeoutMillis = SnapshotDescriptionUtils.getMaxRegionTimeout(conf, Type.TIMESTAMP);
    taskManager = new SnapshotSubprocedurePool(parent, conf);
  }

  /**
   * Create a default snapshot handler - uses a zookeeper based cohort controller.
   * @param conf configuration to use for extracting information like thread pool properties and
   *          frequency to check for errors (wake frequency).
   * @param rss region server running the handler
   * @throws KeeperException if the zookeeper cluster cannot be reached
   */
  public RegionServerSnapshotManager(RegionServerServices rss)
      throws KeeperException {
    super(rss.getConfiguration());
    this.rss = rss;
    ZooKeeperWatcher zkw = rss.getZooKeeper();
    String nodeName = rss.getServerName().toString();
    this.memberRpcs = new ZKProcedureMemberRpcs(zkw,
        SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION, nodeName);

    // read in the snapshot request configuration properties
    Configuration conf = rss.getConfiguration();
    wakeMillis = conf.getLong(SNAPSHOT_REQUEST_WAKE_MILLIS_KEY, SNAPSHOT_REQUEST_WAKE_MILLIS_DEFAULT);
    timeoutMillis = SnapshotDescriptionUtils.getMaxRegionTimeout(conf, Type.TIMESTAMP);
    long keepAlive = conf.getLong(SNAPSHOT_TIMEOUT_MILLIS_KEY,
      SNAPSHOT_TIMEOUT_MILLIS_DEFAULT);
    int opThreads = conf.getInt(SNAPSHOT_REQUEST_THREADS_KEY, SNAPSHOT_REQUEST_THREADS_DEFAULT);
    // create the actual cohort member
    ThreadPoolExecutor pool = ProcedureMember.defaultPool(wakeMillis, keepAlive, opThreads, nodeName);
    this.member = new ProcedureMember(memberRpcs, pool, new SnapshotSubprocedureBuilder());

    // setup the task manager to run all the snapshots tasks
    taskManager = new SnapshotSubprocedurePool(rss, conf);
  }

  /**
   * Start accepting snapshot requests.
   */
  public void start() {
    this.memberRpcs.start(member);
  }

  /**
   * Close <tt>this</tt> and all running snapshot tasks
   * @param force forcefully stop all running tasks
   * @throws IOException
   */
  public void close(boolean force) throws IOException {
    if (force) {
      this.abort("Forcefully closing  - all tasks must stop immediately.", null);
      return;
    }
    // otherwise, let tasks end gracefully
    this.close();
  }

  @Override
  public void close() throws IOException {
    IOException exception = null;
    try {
      this.memberRpcs.close();
    } catch (IOException e) {
      exception = e;
    }
    this.taskManager.close();
    this.member.close();
    if (exception != null) throw exception;
  }

  @Override
  public void abort(String why, Throwable e)  {
    if (this.aborted) return;

    this.aborted = true;
    LOG.warn("Aborting because: " + why, e);
    this.taskManager.abort(why, e);
    try {
      this.member.close();
      this.memberRpcs.close();
    } catch (IOException e1) {
      LOG.error("Failed to cleanly close snapshot controller", e1);
    }
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  /**
   * If in a running state, creates the specified subprocedure for handling an online snapshot.
   *
   * TODO Because this gets the local list of regions to snapshot and not the set the master had,
   * there is a possibility of a race where regions may be missed.  This detected by the master in
   * the snapshot verification step.
   *
   * @param snapshot
   * @return Subprocedure to submit to the ProcedureMemeber.
   */
  public Subprocedure buildSubprocedure(SnapshotDescription snapshot) {

    // don't run a snapshot if the parent is stop(ping)
    if (rss.isStopping() || rss.isStopped()) {
      throw new IllegalStateException("Can't start snapshot on RS: " + rss.getServerName()
          + ", because stopping/stopped!");
    }

    // don't run a new snapshot if we have aborted
    if (aborted) {
      throw new IllegalStateException("Not starting new snapshot becuase aborting.");
    }

    // check to see if this server is hosting any regions for the snapshots
    // check to see if we have regions for the snapshot
    List<HRegion> involvedRegions;
    try {
      involvedRegions = getRegionsToSnapshot(snapshot);
    } catch (IOException e1) {
      throw new IllegalStateException("Failed to figure out if we should handle a snapshot - "
          + "something has gone awry with the online regions.", e1);
    }
    // if we aren't involved, don't run an operation
    if (involvedRegions == null || involvedRegions.size() == 0) return null;

    LOG.debug("Attempting to build new snapshot for: " + snapshot);
    ForeignExceptionDispatcher errorDispatcher = new ForeignExceptionDispatcher();
    switch (snapshot.getType()) {
    case LOGROLL:
      return new LogRollSnapshotSubprocedure(member, errorDispatcher, wakeMillis,
          timeoutMillis, involvedRegions, snapshot, rss.getConfiguration(),
          taskManager, rss.getFileSystem());
    case FLUSH:
      return new FlushSnapshotSubprocedure(member, errorDispatcher, wakeMillis, 
          timeoutMillis, involvedRegions, snapshot, taskManager);
    case GLOBAL:
      throw new IllegalArgumentException("Unimplememted snapshot type:" + snapshot.getType());
    case TIMESTAMP:
      return new TimestampSnapshotSubprocedure(member, errorDispatcher, wakeMillis,
          timeoutMillis, involvedRegions, snapshot,
          RegionServerSnapshotManager.this.getConf(), taskManager,
          rss.getFileSystem());
    default:
      throw new IllegalArgumentException("Unrecognized snapshot type:" + snapshot.getType());
    }
    
  }

  /**
   * Determine if the snapshot should be handled on this server
   *
   * TODO this is racy -- the master expects a list of regionservers, but the regions get the
   * regions.  This means if a region moves somewhere between the calls we'll miss some regions.
   * For example, a region move during a snapshot could result in a region to be skipped or done
   * twice.  This is manageable because the {@link MasterSnapshotVerifier} will double check the
   * region lists after the online portion of the snapshot completes and will explicitly fail the
   * snapshot.
   * 
   * @param snapshot
   * @return the list of online regions. Empty list is returned if no regions are responsible for
   *         the given snapshot.
   * @throws IOException
   */
  private List<HRegion> getRegionsToSnapshot(SnapshotDescription snapshot) throws IOException {
    byte[] table = Bytes.toBytes(snapshot.getTable());
    return rss.getOnlineRegions(table);
  }

  /**
   * Build the actual snapshot runner that will do all the 'hard' work
   */
  public class SnapshotSubprocedureBuilder implements SubprocedureFactory {

    @Override
    public Subprocedure buildSubprocedure(String name, byte[] data) {
      try {
        // unwrap the snapshot information
        SnapshotDescription snapshot = SnapshotDescription.parseFrom(data);
        return RegionServerSnapshotManager.this.buildSubprocedure(snapshot);
        
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException("Could not read snapshot information from request.");
      }
    }

  }

}
