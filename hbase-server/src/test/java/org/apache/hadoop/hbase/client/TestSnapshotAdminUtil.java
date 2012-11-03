/**
 *
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtilsForTests;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotExistsException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;


/**
 * Class to test HBaseAdmin snapshot util functionality
 * e.g. listSnapshot(), deleteSnapshot(), renameSnapshot(), ...
 */
@Category(LargeTests.class)
public class TestSnapshotAdminUtil {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] snapshotName0;
  private byte[] snapshotName1;
  private byte[] snapshotName2;
  private byte[] tableName;
  private HBaseAdmin admin;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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

  @Before
  public void setUp() throws Exception {
    this.admin = TEST_UTIL.getHBaseAdmin();

    long tid = System.currentTimeMillis();
    tableName = Bytes.toBytes("testtb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table
    createTable(tableName, FAMILY);
    loadData(tableName, 1000, FAMILY);
    admin.disableTable(tableName);
  }

  @After
  public void tearDown() throws Exception {
    admin.deleteTable(tableName);
  }

  // ==========================================================================
  //  Test List Snapshot Operation
  // ==========================================================================
  @Test
  public void testListSnapshots() throws IOException {
    List<SnapshotDescription> snapshots;

    snapshots = admin.listSnapshots();
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName2)));

    // Take a snapshots
    admin.snapshot(snapshotName0, tableName);
    snapshots = admin.listSnapshots();
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName2)));

    // Take another snapshot
    admin.snapshot(snapshotName1, tableName);
    snapshots = admin.listSnapshots();
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName2)));

    // Delete a snapshot
    admin.deleteSnapshot(snapshotName1);
    snapshots = admin.listSnapshots();
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName2)));

    // Take another snapshot
    admin.snapshot(snapshotName2, tableName);
    snapshots = admin.listSnapshots();
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName2)));

    // Delete a snapshot
    admin.deleteSnapshot(snapshotName0);
    snapshots = admin.listSnapshots();
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName2)));

    // Delete a snapshot
    admin.deleteSnapshot(snapshotName2);
    snapshots = admin.listSnapshots();
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName2)));
  }

  // ==========================================================================
  //  Test Snapshot Delete Operation
  // ==========================================================================
  @Test(expected=SnapshotDoesNotExistException.class)
  public void testSnapshotDeleteNotExistent() throws IOException {
    // Rename a snapshot that don't exists
    admin.deleteSnapshot(snapshotName1);
  }

  @Test
  public void testSnapshotDelete() throws IOException {
    admin.snapshot(snapshotName0, tableName);
    assertTrue(hasSnapshotNamed(Bytes.toString(snapshotName0)));

    admin.deleteSnapshot(snapshotName0);
    assertFalse(hasSnapshotNamed(Bytes.toString(snapshotName0)));
  }

  // ==========================================================================
  //  Test Snapshot Rename Operation
  // ==========================================================================
  @Test(expected=SnapshotDoesNotExistException.class)
  public void testSnapshotRenameNotExistent() throws IOException {
    // Rename a snapshot that don't exists
    admin.renameSnapshot(snapshotName1, snapshotName1);
  }

  @Test
  public void testSnapshotRename() throws IOException {
    // Take a snapshot
    admin.snapshot(snapshotName0, tableName);
    assertTrue(hasSnapshotNamed(Bytes.toString(snapshotName0)));

    try {
      // Rename a Snapshot
      admin.renameSnapshot(snapshotName0, snapshotName1);
      List<SnapshotDescription> snapshots = admin.listSnapshots();
      assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
      assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
    } finally {
      admin.deleteSnapshot(snapshotName1);
    }
  }

  @Test(expected=SnapshotExistsException.class)
  public void testSnapshotRenameOnExistent() throws IOException {
    // Take a snapshot
    admin.snapshot(snapshotName0, tableName);
    assertTrue(hasSnapshotNamed(Bytes.toString(snapshotName0)));

    // Rename a snapshot (but same name exists)
    admin.renameSnapshot(snapshotName0, snapshotName0);

    admin.deleteSnapshot(snapshotName0);
  }

  @Test
  public void testSnapshotRenameOnDeleted() throws IOException {
    // Take two snapshots
    admin.snapshot(snapshotName0, tableName);
    admin.snapshot(snapshotName1, tableName);
    List<SnapshotDescription> snapshots = admin.listSnapshots();
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertTrue(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));

    // Delete the first one
    admin.deleteSnapshot(snapshotName0);
    assertFalse(hasSnapshotNamed(Bytes.toString(snapshotName0)));

    // Rename the second one as the first one
    admin.renameSnapshot(snapshotName1, snapshotName0);
    assertTrue(hasSnapshotNamed(Bytes.toString(snapshotName0)));

    admin.deleteSnapshot(snapshotName0);
    snapshots = admin.listSnapshots();
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName0)));
    assertFalse(hasSnapshotNamed(snapshots, Bytes.toString(snapshotName1)));
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private boolean hasSnapshotNamed(final String snapshotName) throws IOException {
    List<SnapshotDescription> snapshots = admin.listSnapshots();
    return hasSnapshotNamed(snapshots, snapshotName);
  }

  private boolean hasSnapshotNamed(final List<SnapshotDescription> snapshots,
      final String snapshotName) {
    for (SnapshotDescription snapshot: snapshots) {
      if (snapshotName.equals(snapshot.getName())) {
        return true;
      }
    }
    return false;
  }

  private void createTable(final byte[] tableName, final byte[]... families) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family: families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    admin.createTable(htd, null);
  }

  public void loadData(final byte[] tableName, int rows, byte[]... families) throws IOException {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    byte[] qualifier = Bytes.toBytes("q");
    table.setAutoFlush(false);
    while (rows-- > 0) {
      byte[] value = Bytes.toBytes(System.currentTimeMillis());
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
