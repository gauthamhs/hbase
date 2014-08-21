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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, MediumTests.class})
public class TestQuotaMaxTables {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");
  private final static String NAMESPACE_DEFAULT = "default";
  private final static String NAMESPACE_TEST = "testQuota";

  private final static String[] TABLE_NAMES = new String[] {
    "TestQuotaMaxTables0",
    "TestQuotaMaxTables1",
    "TestQuotaMaxTables2",
  };

  private static HTable[] tables;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, 2000);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);

    TEST_UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(NAMESPACE_TEST).build());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout=90000)
  public void testNamespaceLimit() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();

    // Unlimited table creations
    assertEquals(TABLE_NAMES.length, createTables(NAMESPACE_DEFAULT));
    deleteAllTables(NAMESPACE_DEFAULT);

    // limit to 2 tables in this namespace
    admin.setQuota(QuotaSettingsFactory.namespaceMaxTables(NAMESPACE_DEFAULT, 2));
    assertEquals(2, createTables(NAMESPACE_DEFAULT));
    deleteAllTables(NAMESPACE_DEFAULT);

    // Unlimited table creation on test namespace
    assertEquals(TABLE_NAMES.length, createTables(NAMESPACE_TEST));
    deleteAllTables(NAMESPACE_TEST);

    // unlimit the namespace max
    admin.setQuota(QuotaSettingsFactory.namespaceMaxTables(NAMESPACE_DEFAULT, Integer.MAX_VALUE));
    assertEquals(TABLE_NAMES.length, createTables(NAMESPACE_DEFAULT));
    deleteAllTables(NAMESPACE_DEFAULT);
  }

  @Test(timeout=90000)
  public void testUserLimit() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();

    // Unlimited table creations
    assertEquals(TABLE_NAMES.length, createTables(NAMESPACE_DEFAULT));
    deleteAllTables(NAMESPACE_DEFAULT);

    // limit to 2 tables for this user
    admin.setQuota(QuotaSettingsFactory.userMaxTables(userName, 2));
    assertEquals(2, createTables(NAMESPACE_DEFAULT));
    assertEquals(0, createTables(NAMESPACE_TEST));
    deleteAllTables(NAMESPACE_DEFAULT);

    // unlimit the namespace max
    admin.setQuota(QuotaSettingsFactory.userMaxTables(userName, Integer.MAX_VALUE));
    assertEquals(TABLE_NAMES.length, createTables(NAMESPACE_DEFAULT));
    deleteAllTables(NAMESPACE_DEFAULT);
  }

  private int createTables(String namespace) throws IOException {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    int count = 0;
    try {
      for (String tableName: TABLE_NAMES) {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(namespace, tableName));
        desc.setOwnerString(User.getCurrent().getShortName());
        desc.addFamily(new HColumnDescriptor(FAMILY));
        admin.createTable(desc);
        count++;
      }
    } catch (QuotaExceededException e) {
      LOG.info("Unable to create table " + (count + 1), e);
    }
    return count;
  }

  private void deleteAllTables(String namespace) throws IOException {
    for (String tableName: TABLE_NAMES) {
      TEST_UTIL.deleteTableIfAny(TableName.valueOf(namespace, tableName));
    }
  }
}
