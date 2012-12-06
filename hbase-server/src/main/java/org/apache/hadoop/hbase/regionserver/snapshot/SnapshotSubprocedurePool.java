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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;

/**
 * Handle running each of the individual tasks for completing a snapshot on a regionserver.
 */
public class SnapshotSubprocedurePool implements Closeable, Abortable {
  private static final Log LOG = LogFactory.getLog(SnapshotSubprocedurePool.class);

  /** Maximum number of concurrent snapshot region tasks that can run concurrently */
  private static final String CONCURENT_SNAPSHOT_TASKS_KEY = "hbase.snapshot.region.concurrentTasks";
  private static final int DEFAULT_CONCURRENT_SNAPSHOT_TASKS = 3;

  private final ExecutorCompletionService<Void> taskPool;
  private final ThreadPoolExecutor executor;
  private volatile boolean aborted;
  private final List<Future<Void>> futures = new ArrayList<Future<Void>>();

  public SnapshotSubprocedurePool(Server parent, Configuration conf) {
    // configure the executor service
    long keepAlive = conf.getLong(
      RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_KEY,
      RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT);
    int threads = conf.getInt(CONCURENT_SNAPSHOT_TASKS_KEY, DEFAULT_CONCURRENT_SNAPSHOT_TASKS);
    executor = new ThreadPoolExecutor(1, threads, keepAlive, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory("rs("
            + parent.getServerName().toString() + ")-snapshot-pool"));
    taskPool = new ExecutorCompletionService<Void>(executor);
  }

  /**
   * Submit a task to the pool.
   */
  public void submitTask(final Callable<Void> task) {
    Future<Void> f = this.taskPool.submit(task);
    futures.add(f);
  }

  /**
   * Wait for all of the currently outstanding tasks submitted via {@link #submitTask(Callable)}
   *
   * TODO: With the futures implementation, we may get "stuck" waiting for the slowest task even
   * though a faster one may have errored out.  For a first cut, we'll live with this approach
   * since it sufficient, simple, and correct.
   *
   * @return <tt>true</tt> on success, <tt>false</tt> otherwise
   * @throws SnapshotCreationException if the snapshot failed while we were waiting
   */
  public boolean waitForOutstandingTasks() throws ForeignException {
    LOG.debug("Waiting for snapshot to finish.");

    try {
      for (Future<Void> f: futures) {
          f.get();
      }
      return true;
    } catch (InterruptedException e) {
      if (aborted) throw new ForeignException(
          "Interrupted and found to be aborted while waiting for tasks!", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ForeignException) {
        throw (ForeignException)e.getCause();
      }
      throw new ForeignException(e.getCause());
    } finally {
      // close off remaining tasks
      for (Future<Void> f: futures) {
        if (!f.isDone()){
          f.cancel(true);
        }
      }
    }
    return false;
  }

  /**
   * Attempt to cleanly shutdown any running tasks - allows currently running tasks to cleanly
   * finish
   */
  @Override
  public void close() {
    executor.shutdown();
  }

  @Override
  public void abort(String why, Throwable e) {
    if (this.aborted) return;

    this.aborted = true;
    LOG.warn("Aborting because: " + why, e);
    this.executor.shutdownNow();
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }
}