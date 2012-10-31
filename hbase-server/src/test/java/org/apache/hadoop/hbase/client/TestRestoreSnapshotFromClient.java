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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.*;
import org.junit.experimental.categories.Category;

/**
 * Test clone/restore snapshots from the client
 */
@Category(LargeTests.class)
public class TestRestoreSnapshotFromClient {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] snapshotName0;
  private byte[] snapshotName1;
  private byte[] snapshotName2;
  private int snapshot0Rows;
  private int snapshot1Rows;
  private byte[] tableName;
  private HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Initialize the tests with a table filled with some data
   * and two snapshots (snapshotName0, snapshotName1) of different states.
   * The tableName, snapshotNames and the number of rows in the snapshot are initialized.
   */
  @Before
  public void setup() throws Exception {
    this.admin = TEST_UTIL.getHBaseAdmin();

    long tid = System.currentTimeMillis();
    tableName = Bytes.toBytes("testtb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table and disable it
    createTable(tableName, FAMILY);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    loadData(table, 500, FAMILY);
    snapshot0Rows = TEST_UTIL.countRows(table);
    admin.disableTable(tableName);

    // take a snapshot
    admin.snapshot(snapshotName0, tableName);

    // enable table and insert more data
    admin.enableTable(tableName);
    loadData(table, 500, FAMILY);
    snapshot1Rows = TEST_UTIL.countRows(table);
    admin.disableTable(tableName);

    // take a snapshot of the updated table
    admin.snapshot(snapshotName1, tableName);

    // re-enable table
    admin.enableTable(tableName);
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.deleteSnapshot(snapshotName0);
    admin.deleteSnapshot(snapshotName1);
  }

  @Test
  public void testRestoreSnapshot() throws IOException {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    assertEquals(snapshot1Rows, TEST_UTIL.countRows(table));

    // Restore from snapshot-0
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    assertEquals(snapshot0Rows, TEST_UTIL.countRows(table));

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    assertEquals(snapshot1Rows, TEST_UTIL.countRows(table));
  }

  @Test(expected=SnapshotDoesNotExistException.class)
  public void testCloneNonExistentSnapshot() throws IOException {
    String snapshotName = "random-snapshot-" + System.currentTimeMillis();
    String tableName = "random-table-" + System.currentTimeMillis();
    admin.cloneSnapshot(snapshotName, tableName);
  }

  @Test
  public void testCloneSnapshot() throws IOException {
    byte[] clonedTableName = Bytes.toBytes("clonedtb-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
  }

  private void testCloneSnapshot(final byte[] tableName, final byte[] snapshotName,
      int snapshotRows) throws IOException {
    // create a new table from snapshot
    admin.cloneSnapshot(snapshotName, tableName);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    assertEquals(snapshotRows, TEST_UTIL.countRows(table));

    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testRestoreSnapshotOfCloned() throws IOException {
    byte[] clonedTableName = Bytes.toBytes("clonedtb-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), clonedTableName);
    assertEquals(snapshot0Rows, TEST_UTIL.countRows(table));
    admin.disableTable(clonedTableName);
    admin.snapshot(snapshotName2, clonedTableName);
    admin.deleteTable(clonedTableName);

    admin.cloneSnapshot(snapshotName2, clonedTableName);
    table = new HTable(TEST_UTIL.getConfiguration(), clonedTableName);
    assertEquals(snapshot0Rows, TEST_UTIL.countRows(table));
    admin.disableTable(clonedTableName);
    admin.deleteTable(clonedTableName);
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private void createTable(final byte[] tableName, final byte[]... families) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family: families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    byte[][] splitKeys = new byte[16][];
    byte[] hex = Bytes.toBytes("0123456789abcdef");
    for (int i = 0; i < 16; ++i) {
      splitKeys[i] = new byte[] { hex[i] };
    }
    admin.createTable(htd, splitKeys);
  }

  public void loadData(final HTable table, int rows, byte[]... families) throws IOException {
    byte[] qualifier = Bytes.toBytes("q");
    table.setAutoFlush(false);
    while (rows-- > 0) {
      byte[] value = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), Bytes.toBytes(rows));
      byte[] key = Bytes.toBytes(MD5Hash.getMD5AsHex(value));
      Put put = new Put(key);
      put.setWriteToWAL(false);
      for (byte[] family: families) {
        put.add(family, qualifier, value);
      }
      table.put(put);
    }
    table.flushCommits();
  }
}
