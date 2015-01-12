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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.ipc.RequestContext;

@InterfaceAudience.Private
public class MasterProcedureEnv {
  private static final Log LOG = LogFactory.getLog(MasterProcedureEnv.class);

  private final MasterProcedureQueue procQueue;
  private final MasterServices master;

  public MasterProcedureEnv(final MasterServices master) {
    this.master = master;
    this.procQueue = new MasterProcedureQueue(master.getTableLockManager());
  }

  public User getRequestUser() throws IOException {
    if (RequestContext.isInRequestContext()) {
      return RequestContext.getRequestUser();
    } else {
      return UserProvider.instantiate(getMasterConfiguration()).getCurrent();
    }
  }

  public MasterServices getMasterServices() {
    return master;
  }

  public Configuration getMasterConfiguration() {
    return master.getConfiguration();
  }

  public MasterCoprocessorHost getMasterCoprocessorHost() {
    return master.getMasterCoprocessorHost();
  }

  public MasterProcedureQueue getProcedureQueue() {
    return procQueue;
  }

  public boolean createTable(final TableName table) throws IOException {
    // TODO
    return !MetaTableAccessor.tableExists(master.getConnection(), table);
  }

  public boolean deleteTable(final TableName table) throws IOException {
    master.getMasterQuotaManager().removeTableFromNamespaceQuota(table);

    // TODO
    return MetaTableAccessor.tableExists(master.getConnection(), table);
  }
}
