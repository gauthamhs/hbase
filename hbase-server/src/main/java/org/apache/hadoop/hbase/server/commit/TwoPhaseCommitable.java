/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.server.commit;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;

/**
 * Interface for any class that can be run as a {@link TwoPhaseCommit}. Generally, this is going to
 * be used by things like {@link ThreePhaseCommit}.
 * @param <E> type of exception that can be thrown from the commit phases
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TwoPhaseCommitable<E extends Exception> extends Callable<Void>, Runnable {

  /**
   * When the {@link CommitPhase#PREPARE} phase completes, the latch returned here is counted down.
   * External/internal processes may also count-down this latch, so one must be aware of the count
   * prepare latch size passed in the constructor
   * @return latch counted down on complete of the prepare phase
   */
  public CountDownLatch getPreparedLatch();

  /**
   * The {@link CommitPhase#COMMIT} phase waits for this latch to reach zero. Some process (internal
   * or external) needs to count-down this latch to complete the operation.
   * @return latch blocking the commit phase
   */
  public CountDownLatch getAllowCommitLatch();

  /**
   * This latch is counted down after the {@link CommitPhase#COMMIT} phase completes.
   * @return the commit finished latch
   */
  public CountDownLatch getCommitFinishedLatch();

  /**
   * This latch is counted down after the {@link #finish()} phase completes
   * @return latch for completion of the overall operation.
   */
  public CountDownLatch getCompletedLatch();

  /**
   * Prepare to commit the operation ({@link CommitPhase#PREPARE}). Isn't run if the monitor
   * presents an error when <tt>this</tt> is started.
   * @throws E on failure. Causes the {@link #cleanup(Exception)} phase to run with this exception
   */
  public void prepare() throws E;

  /**
   * Run the {@link CommitPhase#COMMIT} operation. Only runs after the
   * {@link #getAllowCommitLatch()} reaches zero.
   * @throws E on failure. Causes the {@link #cleanup(Exception)} phase to run with this exception.
   */
  public void commit() throws E;

  /**
   * Cleanup from a failure.
   * <p>
   * This method is called only if one of the earlier phases threw an error or an error was found in
   * the error monitor.
   * @param e exception thrown, either from an external source (found via
   *          {@link ExceptionSnare#failOnError()} or from the errors in internal operations,
   *          {@link #prepare()} {@link #commit()}.
   */
  public void cleanup(Exception e);

  /**
   * Cleanup any state that may have changed from the start, {@link #prepare()} to {@link #commit()}
   * . This is guaranteed to run under failure situations after the operation has been started (but
   * not necessarily after {@link #prepare()} has been called).
   */
  public void finish();
}