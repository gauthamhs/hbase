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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/*
 * Head: | 3bit type | 3bit procId len-1 | 2bit size-1 |
 *
 *  Insert
 * +----------------------------+
 * | 111 --- 11 | num-sub-proc  |
 * +----------------------------+
 * | procIds head | procIds     |
 * | sizes head   | sizes       |
 * +----------------------------+
 * | data                       |
 * | data                       |
 * +----------------------------+
 *
 *  Update
 * +----------------------------+
 * | 111 111 11 | procId | size |
 * +----------------------------+
 * | data                       |
 * +----------------------------+
 *
 *  Delete
 * +----------------------------+
 * | 111 111 -- | procId        |
 * +----------------------------+
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureWALHeader {
  private long minProcId;
  private long logId;
  private int version;
  private int type;

  public void setVersion(final int version) {
    this.version = version;
  }

  public int getVersion() {
    return version;
  }

  public void setType(final int type) {
    this.type = type;
  }

  public int getType() {
    return type;
  }

  public void setLogId(final long logId) {
    this.logId = logId;
  }

  public long getLogId() {
    return logId;
  }

  public void setMinProcId(final long minProcId) {
    this.minProcId = minProcId;
  }

  public long getMinProcId() {
    return minProcId;
  }
}