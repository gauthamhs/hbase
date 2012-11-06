package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotExistsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSnapshotRename {
  private static final Log LOG = LogFactory.getLog(TestSnapshotFromClient.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final String STRING_FAMILY_NAME = "fam";
  private static final byte[] TEST_FAM = Bytes.toBytes(STRING_FAMILY_NAME);
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  private static final String firstSnapshot = "firstSnapshot";
  private static final String secondSnapshot = "secondSnapshot";

  /**
   * Setup the config for the cluster
   *
   * @throws Exception
   *             on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // drop the number of attempts for the hbase admin
    conf.setInt("hbase.client.retries.number", 1);
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);
    // and cleanup the archive directory
    try {
      UTIL.getTestFileSystem().delete(
          new Path(UTIL.getDefaultRootDirPath(), ".archive"), true);
    } catch (IOException e) {
      LOG.warn("Failure to delete archive directory", e);
    }
    //TODO: Add the snapshot cleaner back in
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
  /**
   * Verify that the old snapshot is not accessible after a rename.
   * @throws Exception
   */
  public void testBasicRename() throws Exception {

    HBaseAdmin admin = UTIL.getHBaseAdmin();

    FileSystem fs = UTIL.getHBaseCluster().getMaster()
            .getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem()
            .getRootDir();

    SnapshotTestingUtils.createSnapshotAndValidate(admin, STRING_TABLE_NAME, STRING_FAMILY_NAME, firstSnapshot, rootDir, fs, true);
    //then rename that snapshot

    admin.renameSnapshot(firstSnapshot, secondSnapshot);

    //assert that the old directory is not there and that the error messages are correct
    try {
      admin.restoreSnapshot(firstSnapshot);
      Assert.fail("We were able to restore " + firstSnapshot + ", which should not longer exist.");
    } catch (RestoreSnapshotException rse) {
      Assert.assertEquals("Unable to find the table name for snapshot=" + firstSnapshot, rse.getLocalizedMessage());
    } finally {
      admin.enableTable(TABLE_NAME);
      admin.deleteSnapshot(secondSnapshot);
    }
  }

  @Test
  /**
   * Verify that renaming a snapshot to an existing snapshot name is not permitted.
   * @throws Exception
   */
  public void testRenameToExistingSnapshot() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();

    SnapshotTestingUtils.createSnapshotAndValidate(admin, STRING_TABLE_NAME, STRING_FAMILY_NAME, firstSnapshot, rootDir, fs, true);
    //then rename that snapshot

    SnapshotTestingUtils.createSnapshotAndValidate(admin, STRING_TABLE_NAME, STRING_FAMILY_NAME, secondSnapshot, rootDir, fs, true);
    //then rename that snapshot

    // assert that the old directory is not there and that the error
    // messages are correct
    try {
      admin.renameSnapshot(firstSnapshot, secondSnapshot);
      Assert.fail("We were able to rename " + firstSnapshot + ", to " + secondSnapshot + " which already exists.");
    } catch (SnapshotExistsException see) {
      Assert.assertTrue("An incorrect error message was produced for this rename operation. " + see.getLocalizedMessage(), see.getLocalizedMessage().contains("A Snapshot named '" + secondSnapshot +"' already exists."));
    } finally {
      admin.deleteSnapshot(firstSnapshot);
      admin.deleteSnapshot(secondSnapshot);
    }
  }

  @Test
  /**
   * Verify that it is possible to rename a snapshot to a name that was previously deleted
   */
  public void testRenameToDeletedSnapshot() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();

    //create a snapshot of the table
    SnapshotTestingUtils.createSnapshotAndValidate(admin, STRING_TABLE_NAME, STRING_FAMILY_NAME, firstSnapshot, rootDir, fs, true);

    //create snapshot and then delete it
    SnapshotTestingUtils.createSnapshotAndValidate(admin, STRING_TABLE_NAME, STRING_FAMILY_NAME, secondSnapshot, rootDir, fs, true);
    admin.deleteSnapshot(secondSnapshot);

    admin.renameSnapshot(firstSnapshot, secondSnapshot);

    //verify that all is well with that snapshot
    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertExistsMatchingSnapshot(admin, secondSnapshot, STRING_TABLE_NAME);
    Assert.assertEquals("Incorrect number of matching snapshots", 1, snapshots.size());
    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), TABLE_NAME, TEST_FAM, rootDir, admin, fs, false, null, true, null);
  }
}
