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

package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.QuotaUsage;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleType;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.QuotaScope;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeUnit;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Master Quota Manager.
 * It is responsible for initialize the quota table on the first-run and
 * provide the admin operations to interact with the quota table.
 *
 * TODO: FUTURE: The master will be responsible to notify each RS of quota changes
 * and it will do the "quota aggregation" when the QuotaScope is CLUSTER.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterQuotaManager {
  private static final Log LOG = LogFactory.getLog(MasterQuotaManager.class);

  private final MasterServices masterServices;
  private NamedLock<String> namespaceLocks;
  private NamedLock<TableName> tableLocks;
  private NamedLock<String> userLocks;
  private boolean enabled = false;

  public MasterQuotaManager(final MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  public void start() throws IOException {
    // If the user doesn't want the quota support skip all the initializations.
    if (!QuotaUtil.isQuotaEnabled(masterServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }

    // Create the quota table if missing
    if (!MetaTableAccessor.tableExists(masterServices.getShortCircuitConnection(),
          QuotaUtil.QUOTA_TABLE_NAME)) {
      LOG.info("Quota table not found. Creating...");
      createQuotaTable();
    }

    LOG.info("Initializing quota support");
    namespaceLocks = new NamedLock<String>();
    tableLocks = new NamedLock<TableName>();
    userLocks = new NamedLock<String>();

    LOG.debug("Initializing quota master coprocessor");
    masterServices.getMasterCoprocessorHost().load(MasterQuotaObserver.class,
      Coprocessor.PRIORITY_SYSTEM, masterServices.getConfiguration());

    enabled = true;
  }

  public void stop() {
  }

  public boolean isQuotaEnabled() {
    return enabled;
  }

  private Configuration getConfiguration() {
    return masterServices.getConfiguration();
  }

  /* ==========================================================================
   *  Admin operations to manage the quota table
   */
  public SetQuotaResponse setQuota(final SetQuotaRequest req)
      throws IOException, InterruptedException {
    checkQuotaSupport();

    if (req.hasUserName()) {
      userLocks.lock(req.getUserName());
      try {
        if (req.hasTableName()) {
          setUserQuota(req.getUserName(), ProtobufUtil.toTableName(req.getTableName()), req);
        } else if (req.hasNamespace()) {
          setUserQuota(req.getUserName(), req.getNamespace(), req);
        } else {
          setUserQuota(req.getUserName(), req);
        }
      } finally {
        userLocks.unlock(req.getUserName());
      }
    } else if (req.hasTableName()) {
      TableName table = ProtobufUtil.toTableName(req.getTableName());
      tableLocks.lock(table);
      try {
        setTableQuota(table, req);
      } finally {
        tableLocks.unlock(table);
      }
    } else if (req.hasNamespace()) {
      namespaceLocks.lock(req.getNamespace());
      try {
        setNamespaceQuota(req.getNamespace(), req);
      } finally {
        namespaceLocks.unlock(req.getNamespace());
      }
    } else {
      throw new DoNotRetryIOException(
        new UnsupportedOperationException("a user, a table or a namespace must be specified"));
    }
    return SetQuotaResponse.newBuilder().build();
  }

  public void setUserQuota(final String userName, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(getConfiguration(), userName);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(getConfiguration(), userName, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConfiguration(), userName);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(userName, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(userName, quotas);
      }
    });
  }

  public void setUserQuota(final String userName, final TableName table,
      final SetQuotaRequest req) throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(getConfiguration(), userName, table);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(getConfiguration(), userName, table, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConfiguration(), userName, table);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(userName, table, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(userName, table, quotas);
      }
    });
  }

  public void setUserQuota(final String userName, final String namespace,
      final SetQuotaRequest req) throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(getConfiguration(), userName, namespace);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(getConfiguration(), userName, namespace, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConfiguration(), userName, namespace);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(userName, namespace, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(userName, namespace, quotas);
      }
    });
  }

  public void setTableQuota(final TableName table, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getTableQuota(getConfiguration(), table);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addTableQuota(getConfiguration(), table, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteTableQuota(getConfiguration(), table);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetTableQuota(table, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetTableQuota(table, quotas);
      }
    });
  }

  public void setNamespaceQuota(final String namespace, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getNamespaceQuota(getConfiguration(), namespace);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addNamespaceQuota(getConfiguration(), namespace, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteNamespaceQuota(getConfiguration(), namespace);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetNamespaceQuota(namespace, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetNamespaceQuota(namespace, quotas);
      }
    });
  }

  private void setQuota(final SetQuotaRequest req, final SetQuotaOperations quotaOps)
      throws IOException, InterruptedException {
    if (req.hasRemoveAll() && req.getRemoveAll() == true) {
      quotaOps.preApply(null);
      quotaOps.delete();
      quotaOps.postApply(null);
      return;
    }

    // Apply quota changes
    Quotas quotas = quotaOps.fetch();
    quotaOps.preApply(quotas);

    Quotas.Builder builder = (quotas != null) ? quotas.toBuilder() : Quotas.newBuilder();
    if (req.hasThrottle()) applyThrottle(builder, req.getThrottle());
    if (req.hasBypassGlobals()) applyBypassGlobals(builder, req.getBypassGlobals());
    if (req.hasMaxTables()) applyMaxTables(builder, req.getMaxTables());

    // Submit new changes
    quotas = builder.build();
    if (QuotaUtil.isEmptyQuota(quotas)) {
      quotaOps.delete();
    } else {
      quotaOps.update(quotas);
    }
    quotaOps.postApply(quotas);
  }

  private static interface SetQuotaOperations {
    Quotas fetch() throws IOException;
    void delete() throws IOException;
    void update(final Quotas quotas) throws IOException;
    void preApply(final Quotas quotas) throws IOException;
    void postApply(final Quotas quotas) throws IOException;
  }

  /* ==========================================================================
   *  Helpers to apply changes to the quotas
   */
  private void applyThrottle(final Quotas.Builder quotas, final ThrottleRequest req)
      throws IOException {
    Throttle.Builder throttle;

    if (req.hasType() && (req.hasTimedQuota() || quotas.hasThrottle())) {
      // Validate timed quota if present
      if (req.hasTimedQuota()) validateTimedQuota(req.getTimedQuota());

      // apply the new settings
      throttle = quotas.hasThrottle() ? quotas.getThrottle().toBuilder() : Throttle.newBuilder();

      switch (req.getType()) {
        case REQUEST_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setReqNum(req.getTimedQuota());
          } else {
            throttle.clearReqNum();
          }
          break;
        case REQUEST_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setReqSize(req.getTimedQuota());
          } else {
            throttle.clearReqSize();
          }
          break;
        case WRITE_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setWriteNum(req.getTimedQuota());
          } else {
            throttle.clearWriteNum();
          }
          break;
        case WRITE_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setWriteSize(req.getTimedQuota());
          } else {
            throttle.clearWriteSize();
          }
          break;
        case READ_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setReadNum(req.getTimedQuota());
          } else {
            throttle.clearReqNum();
          }
          break;
        case READ_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setReadSize(req.getTimedQuota());
          } else {
            throttle.clearReadSize();
          }
          break;
      }
      quotas.setThrottle(throttle.build());
    } else {
      quotas.clearThrottle();
    }
  }

  private void applyBypassGlobals(final Quotas.Builder quotas, boolean bypassGlobals) {
    if (bypassGlobals) {
      quotas.setBypassGlobals(bypassGlobals);
    } else {
      quotas.clearBypassGlobals();
    }
  }

  private void applyMaxTables(final Quotas.Builder quotas, int maxTables) {
    if (maxTables >= 0 && maxTables < Integer.MAX_VALUE) {
      quotas.setMaxTables(maxTables);
    } else {
      quotas.clearMaxTables();
    }
  }

  private void validateTimedQuota(final TimedQuota timedQuota) throws IOException {
    if (timedQuota.getSoftLimit() < 1) {
      throw new DoNotRetryIOException(new UnsupportedOperationException(
          "The throttle limit must be greater then 0, got " + timedQuota.getSoftLimit()));
    }
  }

  /* ==========================================================================
   *  Master Coprocessor
   */
  public static class MasterQuotaObserver extends BaseMasterObserver {
    public MasterQuotaObserver() {
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
        HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
      MasterServices services = ctx.getEnvironment().getMasterServices();
      if (!services.isInitialized() || desc.getTableName().isSystemTable()) return;

      Configuration conf = services.getConfiguration();
      Set<TableName> tables = services.getTableStateManager().getTables();
      String namespace = desc.getTableName().getNamespaceAsString();

      // Verify namespace "maxTables" quota
      // TODO-MAYBE: This can be cached, but since create is rare enough we can avoid that
      Quotas quota = QuotaUtil.getNamespaceQuota(conf, namespace);
      if (quota != null && quota.hasMaxTables()) {
        int ntables = countNamespaceTables(tables, namespace);
        if ((ntables + 1) > quota.getMaxTables()) {
          throw new QuotaExceededException("The table " + desc.getTableName().getNameAsString()
              + "cannot be created as it would exceed maximum number of tables allowed "
              + " in the namespace .");
        }
      }

      // Verify user "maxTables" quota
      String userName = getTableCreator(services, desc);
      if (userName != null) {
        try {
          services.getMasterQuotaManager().userLocks.lock(userName);
          try {
            quota = QuotaUtil.getUserQuota(conf, userName);
            if (quota != null && quota.hasMaxTables()) {
              QuotaUsage usage = QuotaUtil.getUserQuotaUsage(conf, userName);
              if ((usage.getTablesCreated() + 1) > quota.getMaxTables()) {
                throw new QuotaExceededException("The table " + desc.getTableName().getNameAsString()
                    + "cannot be created as it would exceed maximum number of tables allowed "
                    + " for the user.");
              }

              // TODO: This should be in postCreate.. but...
              usage = incTableCreated(usage, 1);
              QuotaUtil.addUserQuotaUsage(conf, userName, usage);
            }
          } finally {
            services.getMasterQuotaManager().userLocks.unlock(userName);
          }
        } catch (InterruptedException e) {
          IOException iie = new InterruptedIOException();
          iie.initCause(e);
          throw iie;
        }
      }
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tableName) throws IOException {
      MasterServices services = ctx.getEnvironment().getMasterServices();
      Configuration conf = services.getConfiguration();
      String userName = getTableCreator(services, tableName);
      if (userName != null) {
        try {
        services.getMasterQuotaManager().userLocks.lock(userName);
          try {
            QuotaUsage usage = QuotaUtil.getUserQuotaUsage(conf, userName);
            usage = incTableCreated(usage, -1);
            QuotaUtil.addUserQuotaUsage(conf, userName, usage);
          } finally {
            services.getMasterQuotaManager().userLocks.unlock(userName);
          }
        } catch (InterruptedException e) {
          IOException iie = new InterruptedIOException();
          iie.initCause(e);
          throw iie;
        }
      }
    }

    private static int countNamespaceTables(final Set<TableName> tables, final String namespace) {
      int count = 0;
      for (TableName table: tables) {
        if (namespace.equals(table.getNamespaceAsString())) {
          count++;
        }
      }
      return count;
    }

    private String getTableCreator(MasterServices services, HTableDescriptor htd) throws IOException {
      // TODO: Replace with HBASE-11996
      return htd.getOwnerString();
    }

    private String getTableCreator(MasterServices services, TableName table) throws IOException {
      // TODO: Replace with HBASE-11996
      return services.getTableDescriptors().get(table).getOwnerString();
    }

    private QuotaUsage incTableCreated(QuotaUsage usage, int inc) {
      QuotaUsage.Builder builder = usage != null ? usage.toBuilder() : QuotaUsage.newBuilder();
      builder.setTablesCreated(Math.max(0, builder.getTablesCreated() + inc));
      return builder.build();
    }
  }

  /* ==========================================================================
   *  Helpers
   */

  private void checkQuotaSupport() throws IOException {
    if (!enabled) {
      throw new DoNotRetryIOException(
        new UnsupportedOperationException("quota support disabled"));
    }
  }

  private void createQuotaTable() throws IOException {
    HRegionInfo newRegions[] = new HRegionInfo[] {
      new HRegionInfo(QuotaUtil.QUOTA_TABLE_NAME)
    };

    masterServices.getExecutorService()
      .submit(new CreateTableHandler(masterServices,
        masterServices.getMasterFileSystem(),
        QuotaUtil.QUOTA_TABLE_DESC,
        masterServices.getConfiguration(),
        newRegions,
        masterServices)
          .prepare());
  }

  private static class NamedLock<T> {
    private HashSet<T> locks = new HashSet<T>();

    public void lock(final T name) throws InterruptedException {
      synchronized (locks) {
        while (locks.contains(name)) {
          locks.wait();
        }
        locks.add(name);
      }
    }

    public void unlock(final T name) {
      synchronized (locks) {
        locks.remove(name);
        locks.notifyAll();
      }
    }
  }
}

