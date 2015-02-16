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

package org.apache.hadoop.hbase.procedure2;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Once a Procedure completes the Procedure Execute will replace the Procedure
 * with a ProcedureResult which is a lightweight version of the procedure metadata
 * that contains only the informations useful for the client.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProcedureResult {
  private final Exception exception;
  private final long timestamp;
  private final byte[] result;

  public ProcedureResult(final long timestamp, final Exception exception) {
    this.timestamp = timestamp;
    this.exception = exception;
    this.result = null;
  }

  public ProcedureResult(final long timestamp, final byte[] result) {
    this.timestamp = timestamp;
    this.exception = null;
    this.result = result;
  }

  public boolean isFailed() {
    return exception != null;
  }

  public Exception getException() {
    return exception;
  }

  public byte[] getResult() {
    return result;
  }

  public long getTimestamp() {
    return timestamp;
  }
}