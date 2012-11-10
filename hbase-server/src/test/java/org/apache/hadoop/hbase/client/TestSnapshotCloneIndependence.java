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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/* Test to verify that the cloned table is independent of the table from which it was cloned */

public class TestSnapshotCloneIndependence {

	private static final Log LOG = LogFactory
			.getLog(TestSnapshotFromClient.class);
	private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
	private static final int NUM_RS = 2;
	private static final String STRING_TABLE_NAME = "test";
	private static final String TEST_FAM_STR = "fam";
	private static final byte[] TEST_FAM = Bytes.toBytes(TEST_FAM_STR);
	private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);

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
		// change the flush size to a small amount, regulating number of store
		// files
		conf.setInt("hbase.hregion.memstore.flush.size", 25000);
		// so make sure we get a compaction when doing a load, but keep around
		// some
		// files in the store
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
	public void testAppendIndependent() throws Exception {

		// Create test-timestamp
		FileSystem fs = UTIL.getHBaseCluster().getMaster()
				.getMasterFileSystem().getFileSystem();
		Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem()
				.getRootDir();

		HBaseAdmin admin = UTIL.getHBaseAdmin();
		final long startTime = System.currentTimeMillis();
		final String localTableNameAsString = STRING_TABLE_NAME + startTime;
		HTable original = UTIL.createTable(Bytes.toBytes(localTableNameAsString), TEST_FAM);
		UTIL.loadTable(original, TEST_FAM);
		final int origTableLineCount =  UTIL.countRows(original);
		
		
		// Take a snapshot
		final String snapshotNameAsString = "snapshot_"
				+ localTableNameAsString;
		byte[] snapshotName = Bytes.toBytes(snapshotNameAsString);

		SnapshotTestingUtils.createSnapshotAndValidate(admin, localTableNameAsString,
				TEST_FAM_STR, snapshotNameAsString, rootDir, fs);
		admin.enableTable(localTableNameAsString);
		
		byte[] cloneTableName = Bytes.toBytes("test-clone-"
				+ localTableNameAsString);
	
		admin.cloneSnapshot(snapshotName, cloneTableName);

		// Attempt to add data to test-timestamp		
		Thread.sleep(500); //not thrilled about this, but the test flaps if the region is offline, which it sometimes is
		
		
		HTable clonedTable = new HTable(UTIL.getConfiguration(), cloneTableName);
		
		// Verify that it not present in the clone
		final int clonedTableLineCount = UTIL.countRows(clonedTable);
		
		Assert.assertEquals("The line counts of original and cloned tables do not match after clone. ", origTableLineCount, clonedTableLineCount);

		// Attempt to add data to the test-timestamp-clone
		final String rowKey = "new-row-" + System.currentTimeMillis();
		
		Put p = new Put(Bytes.toBytes(rowKey));
		p.add(TEST_FAM, Bytes.toBytes("someQualifier"), Bytes.toBytes("someString"));
		original.put(p);
		original.flushCommits();
		
		// Verify that it is not present in the original table
		Assert.assertEquals("The row count of the original table was not modified by the put", origTableLineCount + 1, UTIL.countRows(original));
		Assert.assertEquals("The row count of the cloned table changed as a result of addition to the original", clonedTableLineCount, UTIL.countRows(clonedTable));

		p = new Put(Bytes.toBytes(rowKey));
		p.add(TEST_FAM, Bytes.toBytes("someQualifier"), Bytes.toBytes("someString"));
		clonedTable.put(p);
		
		// Verify that it is not present in the original table
		Assert.assertEquals("The row count of the original table was modified by the put to the clone", origTableLineCount +1, UTIL.countRows(original));
		Assert.assertEquals("The row count of the cloned table was not modified by the put", clonedTableLineCount + 1, UTIL.countRows(clonedTable));
	}

	@Test
	public void testMetadataChangesIndependent() throws Exception {

		// Create test-timestamp
		FileSystem fs = UTIL.getHBaseCluster().getMaster()
				.getMasterFileSystem().getFileSystem();
		Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem()
				.getRootDir();


		//Create a table
		HBaseAdmin admin = UTIL.getHBaseAdmin();
		final long startTime = System.currentTimeMillis();
		final String localTableNameAsString = STRING_TABLE_NAME + startTime;
		HTable original = UTIL.createTable(Bytes.toBytes(localTableNameAsString), TEST_FAM);
		UTIL.loadTable(original, TEST_FAM);
		
		final String snapshotNameAsString = "snapshot_"
				+ localTableNameAsString;
		
		//Create a snapshot
		
		SnapshotTestingUtils.createSnapshotAndValidate(admin, localTableNameAsString,
				  TEST_FAM_STR, snapshotNameAsString, rootDir, fs);
		admin.enableTable(localTableNameAsString);
		
		byte[] cloneTableName = Bytes.toBytes("test-clone-"
				+ localTableNameAsString);
	
		//Clone the snapshot
		byte[] snapshotName = Bytes.toBytes(snapshotNameAsString);
		admin.cloneSnapshot(snapshotName, cloneTableName);
		
		//Add a new column family to the original table
		byte[] TEST_FAM_2 = Bytes.toBytes("fam2");
		HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAM_2);
		
		admin.disableTable(localTableNameAsString);
		admin.addColumn(localTableNameAsString, hcd);
		
		//Verify that it is not in the snapshot
		admin.enableTable(localTableNameAsString);
		
		//get a description of the cloned table
		//get a list of its families
		//assert that the family is there
		
		HTableDescriptor originalTableDescriptor = original.getTableDescriptor();	
		HTableDescriptor clonedTableDescriptor = admin.getTableDescriptor(cloneTableName);
		
		Assert.assertTrue("The original family was not found. There is something wrong. ", originalTableDescriptor.hasFamily(TEST_FAM));
		Assert.assertTrue("The original family was not found in the clone. There is something wrong. ", clonedTableDescriptor.hasFamily(TEST_FAM));
		
		Assert.assertTrue("The new family was not found. ", originalTableDescriptor.hasFamily(TEST_FAM_2));
		Assert.assertTrue("The new family was not found. ", !clonedTableDescriptor.hasFamily(TEST_FAM_2));
	}
}
