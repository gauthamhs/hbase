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
package org.apache.hadoop.hbase.procedure;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.errorhandling.TimeoutExceptionInjector;

/**
 * Factory for a {@link Procedure} to run a coordinator. Subclasses should override
 * {@link #createProcedure(ProcedureCoordinator, String, byte[], List)} to get
 * custom global barrier execution functionality.
 *
 * TODO: this is may not be necessary anymore -- all real custom logic lives in
 * {@link Subprocedure}
 *
 * TODO: pattern problem: this should take builders specific to each type of procedure,
 * and act like a catalog of procedures.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProcedureFactory {
  protected final Object[] timeoutInfo;
  protected final long timeout;
  protected final long wake;

  /**
   * @param wakeFreq frequency in millis to the should check for external exceptions
   * @param timeout max amount of time the procedure should run before failing. See
   *          {@link TimeoutExceptionInjector}
   * @param timeoutInfo information to pass along in the case of a timeout
   *
   * TODO: these timeout values should be part of the procArgs?
   */
  public ProcedureFactory(long wakeFreq, long timeout, Object[] timeoutInfo) {
    this.wake = wakeFreq;
    this.timeout = timeout;
    this.timeoutInfo = timeoutInfo;
  }

  /**
   * Build a Procedure instance for a distributed execution
   * @param parent coordinator running the procedure
   * @param procName name of the procedure (must be unique) (is this per procedure type or per individual execution)
   * @param procArgs Info arguments to pass to members
   * @param expectedMembers names of the expected members
   * @return a {@link Procedure} instance that is ready to run the procedure
   */
  public Procedure createProcedure(ProcedureCoordinator parent,
      String procName, byte[] procArgs, List<String> expectedMembers) {
    return new Procedure(parent, wake, timeout,
        timeoutInfo, procName, procArgs, expectedMembers);
  }
}