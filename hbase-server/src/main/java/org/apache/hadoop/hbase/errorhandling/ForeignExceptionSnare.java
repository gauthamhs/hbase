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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Captures the first exception it receives.  Subsequent exceptions are dropped to prevent
 * possible infinite error propagation. The captured exception can be retrieved wrapped as an
 * {@link ForeignException} via the {@link ForeignExceptionCheckable} interface.
 * <p>
 * This must be thread-safe, because this is expected to be used to propagate exceptions from
 * foreign threads.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ForeignExceptionSnare implements ForeignExceptionCheckable, ForeignExceptionListener {

  private static final Log LOG = LogFactory.getLog(ForeignExceptionSnare.class);
  private boolean error = false;
  private ForeignException exception;
  private String name;

  /**
   * Create an exception snare with a generic error name
   */
  public ForeignExceptionSnare() {
    this.name = "generic-error-snare";
  }

  @Override
  public synchronized void rethrowException() throws ForeignException {
    if (hasException()) {
      throw exception;
    }
  }

  @Override
  public boolean hasException() {
    return this.error;
  }

  @Override
  public ForeignException getException() {
    return exception;
  }

  @Override
  public void receiveError(String message, ForeignException e, Object... info) {
    LOG.error(name + " Got an error: " + message + ", info:" + Arrays.toString(info));
    LOG.debug("Exception debug info", e);
    synchronized (this) {
      // if we already got the error or we received the error fail fast
      if (this.error) return;
      // store the error since we haven't seen it before
      this.error = true;
      if (e == null) {
        exception = new UnknownForeignException("Unexpected Unspecified ForeignException");
      } else {
        exception = e;
      }
    }
  }
}