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
package org.apache.hadoop.hbase.errorhandling;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The dispatcher acts as the state holding entity for foreign error handling.  The first
 * exception received by the dispatcher get passed directly to the listeners.  Subsequent
 * exceptions are logged locally but no propagated. This is particularly useful to help avoid
 * accidentally having infinite loops when passing errors.
 * <p>
 * If there are multiple dispatchers that are all in the same foreign exception monitoring group, 
 * ideally all these monitors are "peers" -- any error on one dispatcher should get propagated to
 * others all others (via rpc, or some other mechanism).  Due to racing error conditions the exact
 * reason for failure may be different on different peers, but the fact that they are in error
 * state should eventually hold on all.
 * <p>
 * This is thread-safe and must be because this is expected to be used to propagate exceptions
 * from foreign threads.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ForeignExceptionDispatcher implements ForeignExceptionListener, ForeignExceptionCheckable {
  private static final Log LOG = LogFactory.getLog(ForeignExceptionDispatcher.class);
  protected final String name;
  protected final List<ForeignExceptionListener> listeners = new ArrayList<ForeignExceptionListener>();
  private final ForeignExceptionSnare snare = new ForeignExceptionSnare();

  public ForeignExceptionDispatcher(String name) {
    this.name = name;
  }

  public ForeignExceptionDispatcher() {
    this("");
  }

  public String getName() {
    return name;
  }

  @Override
  public synchronized void receiveError(String message, ForeignException e, Object... info) {
    // if we already have an error, then ignore it
    if (snare.hasException()) return;

    LOG.debug(name + "Accepting received error:" + message);
    // mark that we got the error
    if (e == null) {
      e = new UnknownForeignException(message);
    }
    snare.receiveError(message, e, info);

    // notify all the listeners
    dispatchError(message, e, info);
  }

  @Override
  public void rethrowException() throws ForeignException {
    snare.rethrowException();
  }

  @Override
  public boolean hasException() {
    return snare.hasException();
  }

  @Override
  synchronized public ForeignException getException() {
    return snare.getException();
  }

  /**
   * Sends an exception to all listeners.
   * @param message human readable message passed to the listener
   * @param e {@link ForeignException} containing the cause.  Can be null.
   * @param info TODO determine if we can get rid of this and fold it into the ForeignException
   */
  private void dispatchError(String message, ForeignException e, Object... info) {
    // update all the listeners with the passed error
    LOG.debug(name + " Recieved error, notifying listeners...");
    for (ForeignExceptionListener l: listeners) {
      l.receiveError(message, e, info);
    }
  }

  /**
   * Listen for failures to a given process.  This method should only be used during
   * initialization and not added to after exceptions are accepted.
   * @param errorable listener for the errors.  may be null.
   */
  public synchronized void addErrorListener(ForeignExceptionListener errorable) {
    this.listeners.add(errorable);
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @param monitor monitor to check for errors while waiting
   * @param wakeFrequency frequency to wake up and check for errors (in
   *          {@link TimeUnit#MILLISECONDS})
   * @param latchDescription description of the latch, for logging
   * @throws ForeignException type of error the monitor can throw, if the task fails
   * @throws InterruptedException if we are interrupted while waiting on latch
   */
  public static void waitForLatch(CountDownLatch latch, ForeignExceptionCheckable monitor,
      long wakeFrequency, String latchDescription) throws ForeignException,
      InterruptedException {
    boolean released = false;
    while (!released) {
      if (monitor != null) {
        monitor.rethrowException();
      }
      LOG.debug("Waiting for '" + latchDescription + "' latch. (sleep:" + wakeFrequency + " ms)");
      released = latch.await(wakeFrequency, TimeUnit.MILLISECONDS);
    }
  }
}