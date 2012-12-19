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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.snapshot.HSnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.exception.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * Test the master-related aspects of a snapshot
 */
@Category(MediumTests.class)
public class TestSnapshotFromMaster {

  private static final Log LOG = LogFactory.getLog(TestSnapshotFromMaster.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static Path rootDir;
  private static Path snapshots;
  private static FileSystem fs;
  private static HMaster master;

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
    fs = UTIL.getDFSCluster().getFileSystem();
    rootDir = FSUtils.getRootDir(UTIL.getConfiguration());
    snapshots = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
    master = UTIL.getMiniHBaseCluster().getMaster();
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 5);
    conf.setInt("hbase.hstore.compactionThreshold", 5);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // drop the number of attempts for the hbase admin
    conf.setInt("hbase.client.retries.number", 1);
  }

  @Before
  public void setup() throws Exception {
    master.getSnapshotManagerForTesting().setSnapshotHandlerForTesting(null);
  }

  @After
  public void tearDown() throws Exception {
    if (!fs.delete(snapshots, true)) {
      throw new IOException("Couldn't delete snapshots directory (" + snapshots
          + " for an unknown reason");
    }
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      // NOOP;
    }
  }

  /**
   * Test that the contract from the master for checking on a snapshot are valid.
   * <p>
   * <ol>
   * <li>If a snapshot fails with an error, we expect to get the source error.</li>
   * <li>If there is no snapshot name supplied, we should get an error.</li>
   * <li>If asking about a snapshot has hasn't occurred, you should get an error.</li>
   * </ol>
   */
  @Test(timeout = 15000)
  public void testIsDoneContract() throws Exception {

    String snapshotName = "asyncExpectedFailureTest";

    // check that we get an exception when looking up snapshot where one hasn't happened
    SnapshotTestingUtils.expectSnapshotDoneException(master, new HSnapshotDescription(),
      UnknownSnapshotException.class);

    // and that we get the same issue, even if we specify a name
    HBaseProtos.SnapshotDescription desc = HBaseProtos.SnapshotDescription.newBuilder()
      .setName(snapshotName).build();
    SnapshotTestingUtils.expectSnapshotDoneException(master, new HSnapshotDescription(desc),
      UnknownSnapshotException.class);

    // set a mock handler to simulate a snapshot
    DisabledTableSnapshotHandler mockHandler = Mockito.mock(DisabledTableSnapshotHandler.class);
    Mockito.when(mockHandler.getExceptionIfFailed()).thenReturn(null);
    Mockito.when(mockHandler.getSnapshot()).thenReturn(desc);
    Mockito.when(mockHandler.isFinished()).thenReturn(new Boolean(true));

    master.getSnapshotManagerForTesting().setSnapshotHandlerForTesting(mockHandler);

    // if we do a lookup without a snapshot name, we should fail - you should always know your name
    SnapshotTestingUtils.expectSnapshotDoneException(master, new HSnapshotDescription(desc),
      UnknownSnapshotException.class);

    // then do the lookup for the snapshot that it is done
    boolean isDone = master.isSnapshotDone(new HSnapshotDescription(desc));
    assertTrue("Snapshot didn't complete when it should have.", isDone);

    // now try the case where we are looking for a snapshot we didn't take
    desc = HBaseProtos.SnapshotDescription.newBuilder().setName("Not A Snapshot").build();
    SnapshotTestingUtils.expectSnapshotDoneException(master, new HSnapshotDescription(desc),
      UnknownSnapshotException.class);

    // then create a snapshot to the fs and make sure that we can find it when checking done
    snapshotName = "completed";
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path root = master.getMasterFileSystem().getRootDir();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, root);
    desc = desc.toBuilder().setName(snapshotName).build();
    SnapshotDescriptionUtils.writeSnapshotInfo(desc, snapshotDir, fs);

    isDone = master.isSnapshotDone(new HSnapshotDescription(desc));
    assertTrue("Completed, on-disk snapshot not found", isDone);

    HBaseSnapshotException testException = new SnapshotCreationException("test fail", desc);
    Mockito.when(mockHandler.getExceptionIfFailed()).thenReturn(testException);
    try {
      master.isSnapshotDone(new HSnapshotDescription(desc));
      fail("Master should have passed along snapshot error, but didn't");
    } catch (IOException e) {
      LOG.debug("Correctly got exception back from the master on failure: " + e.getMessage());
    }
  }

  @Test
  public void testListSnapshots() throws Exception {
    // first check when there are no snapshots
    List<HSnapshotDescription> snapshots = master.listSnapshots();
    assertEquals("Found unexpected number of snapshots", 0, snapshots.size());

    // write one snapshot to the fs
    String snapshotName = "completed";
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    HBaseProtos.SnapshotDescription snapshot = HBaseProtos.SnapshotDescription.newBuilder()
      .setName(snapshotName).build();
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, snapshotDir, fs);

    // check that we get one snapshot
    snapshots = master.listSnapshots();
    assertEquals("Found unexpected number of snapshots", 1, snapshots.size());
    List<HBaseProtos.SnapshotDescription> expected = Lists.newArrayList(snapshot);
    assertEquals("Returned snapshots don't match created snapshots", expected, snapshots);

    // write a second snapshot
    snapshotName = "completed_two";
    snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    snapshot = HBaseProtos.SnapshotDescription.newBuilder().setName(snapshotName).build();
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, snapshotDir, fs);
    expected.add(snapshot);

    // check that we get one snapshot
    snapshots = master.listSnapshots();
    assertEquals("Found unexpected number of snapshots", 2, snapshots.size());
    assertEquals("Returned snapshots don't match created snapshots", expected, snapshots);
  }

  @Test
  public void testDeleteSnapshot() throws Exception {

    String snapshotName = "completed";
    HBaseProtos.SnapshotDescription snapshot = HBaseProtos.SnapshotDescription.newBuilder()
      .setName(snapshotName).build();
    try {
      master.deleteSnapshot(new HSnapshotDescription(snapshot));
      fail("Master didn't throw exception when attempting to delete snapshot that doesn't exist");
    } catch (IOException e) {
      LOG.debug("Correctly failed delete of non-existant snapshot:" + e.getMessage());
    }

    // write one snapshot to the fs
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, snapshotDir, fs);

    // then delete the existing snapshot,which shouldn't cause an exception to be thrown
    master.deleteSnapshot(new HSnapshotDescription(snapshot));
  }
}
