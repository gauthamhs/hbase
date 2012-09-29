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

package org.apache.hadoop.hbase.snapshot.restore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;

public class RestoreSnapshotHelper {
	static final Log LOG = LogFactory.getLog(RestoreSnapshotHelper.class);

  private final Map<byte[], byte[]> regionsMap =
        new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

  private final SnapshotDescription snapshotDesc;
  private final Path snapshotDir;

  private final HTableDescriptor tableDesc;
  private final Path tableDir;

  private final CatalogTracker catalogTracker;
  private final Configuration conf;
  private final FileSystem fs;

  public RestoreSnapshotHelper(final Configuration conf, final FileSystem fs,
      final CatalogTracker catalogTracker,
      final SnapshotDescription SnapshotDescription, final Path snapshotDir,
      final HTableDescriptor tableDescriptor, final Path tableDir)
  {
    this.fs = fs;
    this.conf = conf;
    this.catalogTracker = catalogTracker;
    this.snapshotDesc = SnapshotDescription;
    this.snapshotDir = snapshotDir;
    this.tableDesc = tableDescriptor;
    this.tableDir = tableDir;
  }

  public RestoreSnapshotHelper(final Configuration conf, final FileSystem fs,
      final CatalogTracker catalogTracker,
      final byte[] snapshotName, byte[] tableName) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);

    this.fs = fs;
    this.conf = conf;
    this.catalogTracker = catalogTracker;

    // Initialize Snapshot data path
    this.snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    this.snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

    // Initialze Table data path
    if (tableName == null) tableName = Bytes.toBytes(snapshotDesc.getTable());
    this.tableDir = HTableDescriptor.getTableDir(rootDir, tableName);
    this.tableDesc = FSTableDescriptors.getTableDescriptor(fs, rootDir, tableName);
  }

  /**
   * Restore table to a specified snapshot state.
   */
  public void restore() throws IOException {
    restore(false);
  }

  /**
   * Restore table to a specified snapshot state.
   *
   * @param noWALs Avoid restoring WALs
   */
  public void restore(boolean noWALs) throws IOException {
    Set<String> snapshotRegionNames = getSnapshotRegionNames();
    if (snapshotRegionNames == null) {
      LOG.warn("Nothing to restore. Snapshot " + snapshotDesc + " looks empty");
      return;
    }

    List<HRegionInfo> regionsToRestore = null;
    List<HRegionInfo> regionsToRemove = null;
    List<HRegionInfo> regionsToAdd = null;

    // Identify which region are still available and which not.
    // NOTE: we rely upon the region name as: "table name, start key, end key"
    List<HRegionInfo> tableRegions = getTableRegions();
    if (tableRegions != null && tableRegions.size() > 0) {
      regionsToRestore = new LinkedList<HRegionInfo>();
      regionsToRemove = new LinkedList<HRegionInfo>();

      for (HRegionInfo regionInfo: tableRegions) {
        String regionName = regionInfo.getEncodedName();
        if (snapshotRegionNames.contains(regionName)) {
          LOG.info("region to restore: " + regionName);
          snapshotRegionNames.remove(regionInfo);
          regionsToRestore.add(regionInfo);
        } else {
          LOG.info("region to remove: " + regionName);
          regionsToRemove.add(regionInfo);
        }
      }
    }

    // Regions to Add: present in the snapshot but not in the current table
    if (snapshotRegionNames.size() > 0) {
      regionsToAdd = new LinkedList<HRegionInfo>();
      for (String regionName: snapshotRegionNames) {
        LOG.info("region to add: " + regionName);
        Path regionDir = new Path(snapshotDir, regionName);
        regionsToAdd.add(HRegion.loadDotRegionInfoFileContent(fs, regionDir));
      }
    }

    // Restore regions using the snapshot data
    restoreRegions(regionsToRestore);

    // Remove regions from the current table
    removeRegions(regionsToRemove);

    // Create new regions cloning from the snapshot
    cloneRegions(regionsToAdd);

    // Restore WALs
    if (!noWALs) {
      restoreWALs();
    } else {
      LOG.info("skip restore WALs for " + snapshotDesc.getName());
    }
  }

  /**
   * Restore specified regions by restoring content to the snapshot state.
   */
  private void restoreRegions(final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;
    for (HRegionInfo hri: regions) restoreRegion(hri);
  }

  /**
   * Restore region by removing files not it in the snapshot
   * and adding the missing ones from the snapshot.
   */
  private void restoreRegion(HRegionInfo regionInfo) throws IOException {
    Map<String, List<String>> snapshotFiles = getRegionHFileReferences(regionInfo);

    Path regionDir = new Path(tableDir, regionInfo.getEncodedName());
    String tableName = tableDesc.getNameAsString();

    for (Map.Entry<String, List<String>> familyEntry: snapshotFiles.entrySet()) {
      byte[] family = Bytes.toBytes(familyEntry.getKey());
      Path familyDir = new Path(regionDir, familyEntry.getKey());
      Set<String> familyFiles = getRegionFamilyFiles(familyDir);

      List<String> hfilesToAdd = new LinkedList<String>();
      for (String hfileName: familyEntry.getValue()) {
        if (familyFiles.contains(hfileName)) {
          // HFile already present
          familyFiles.remove(hfileName);
        } else {
          // HFile missing
          hfilesToAdd.add(hfileName);
        }
      }

      // Remove hfiles not present in the snapshot (TODO: Use ArchiveManager)
      for (String hfileName: familyFiles) {
        Path hfile = new Path(familyDir, hfileName);
        LOG.trace("Removing hfile=" + hfile + " from table=" + tableName);
        HFileArchiver.archiveStoreFile(fs, regionInfo, conf, tableDir, family, hfile);
      }

      // Restore Missing files
      for (String hfileName: hfilesToAdd) {
        LOG.trace("Adding HFileLink " + hfileName + " to table=" + tableName);
        HFileLink.create(conf, fs, familyDir, regionInfo, hfileName);
      }
    }
  }

  /**
   * @return The set of files in the specified family directory.
   */
  private Set<String> getRegionFamilyFiles(final Path familyDir) throws IOException {
    Set<String> familyFiles = new HashSet<String>();

    FileStatus[] hfiles = FSUtils.listStatus(fs, familyDir);
    if (hfiles == null) return familyFiles;

    for (FileStatus hfileRef: hfiles) {
      String hfileName = hfileRef.getPath().getName();
      if (HFileLink.isHFileLink(hfileName)) {
        hfileName = HFileLink.getReferencedHFileName(hfileName);
      } else if (Reference.checkReference(hfileRef.getPath())) {
        hfileName = Reference.getDeferencedHFileName(hfileName);
      }
      familyFiles.add(hfileName);
    }

    return familyFiles;
  }

  /**
   * Remove specified regions by removing them from file-system and .META.
   */
  private void removeRegions(final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;
    for (HRegionInfo hri: regions) removeRegion(hri);
  }

  /**
   * Remove region from file-system and .META.
   */
  private void removeRegion(HRegionInfo regionInfo) throws IOException {
    // Remove region from META
    MetaEditor.deleteRegion(catalogTracker, regionInfo);

    // "Delete" region from FS
    HFileArchiver.archiveRegion(fs, regionInfo);
  }

  /**
   * Clone specified regions. For each region create a new region
   * and create a HFileLink for each hfile.
   */
  private void cloneRegions(final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;

    final int batchSize = conf.getInt("hbase.master.createtable.batchsize", 100);
    List<HRegionInfo> createdRegions = new ArrayList<HRegionInfo>(batchSize);

    for (HRegionInfo hri: regions) {
      HRegionInfo clonedHri = cloneRegion(hri);
      createdRegions.add(clonedHri);

      if (createdRegions.size() % batchSize == 0) {
        // Insert into META
        LOG.debug("add " + createdRegions.size() + " regions to .META.");
        MetaEditor.addRegionsToMeta(catalogTracker, createdRegions);
        createdRegions.clear();
      }
    }

    // Insert into META
    if (createdRegions.size() > 0) {
      LOG.debug("add " + createdRegions.size() + " regions to .META.");
      MetaEditor.addRegionsToMeta(catalogTracker, createdRegions);
    }
  }

  /**
   * Clone region from the snapshot info.
   *
   * Each region is encoded with the table name, so the cloned region will have
   * a different region name.
   *
   * Instead of copying the hfiles a HFileLink is created.
   *
   * @param snapshotRegionInfo
   * @return new region info
   */
  private HRegionInfo cloneRegion(HRegionInfo snapshotRegionInfo) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    HRegion region = cloneRegion(snapshotRegionInfo, rootDir, conf, tableDesc);
    HRegionInfo regionInfo = region.getRegionInfo();
    LOG.info("clone region=" + snapshotRegionInfo.getEncodedName() +
             " as " + regionInfo.getEncodedName());
    try {
      Map<String, List<String>> snapshotFiles = getRegionHFileReferences(snapshotRegionInfo);
      cloneHFiles(snapshotRegionInfo, regionInfo, snapshotFiles);
    } finally {
      region.close();
    }
    regionsMap.put(snapshotRegionInfo.getEncodedNameAsBytes(), regionInfo.getEncodedNameAsBytes());
    return regionInfo;
  }

  /**
   * Get the list of hfiles for the specified snapshot region.
   * NOTE: The current implementation keep one Reference file per HFile in a region folder.
   *
   * @param snapshotRegionInfo Snapshot region info
   * @throws IOException
   */
  private Map<String, List<String>> getRegionHFileReferences(final HRegionInfo snapshotRegionInfo)
      throws IOException {
    Path snapshotRegionDir = new Path(snapshotDir, snapshotRegionInfo.getEncodedName());
    Map<String, List<String>> familyFiles = new TreeMap<String, List<String>>();
    for (HColumnDescriptor hcd: tableDesc.getFamilies()) {
      String family = hcd.getNameAsString();

      List<String> hfiles = new LinkedList<String>();
      familyFiles.put(family, hfiles);

      Path familyDir = new Path(snapshotRegionDir, family);
      FileStatus[] familyHFiles = FSUtils.listStatus(fs, familyDir);
      if (familyHFiles == null) continue;

      for (FileStatus hfileRef: familyHFiles) {
        Path hfileRefPath = hfileRef.getPath();
        if (!Reference.isReference(hfileRefPath)) continue;
        hfiles.add(Reference.getDeferencedHFileName(hfileRefPath.getName()));
      }
    }
    return familyFiles;
  }

  /**
   * Clone region hfiles by creating a HFileLink for each hfile in the snapshot.
   * NOTE: The current implementation keep one Reference file per HFile.
   *
   * @param snapshotRegionInfo Snapshot region info
   * @param regionInfo Restore table region info
   * @param snapshotFiles Map that contains file names contained in each family
   * @throws IOException
   */
  public void cloneHFiles(final HRegionInfo snapshotRegionInfo, final HRegionInfo regionInfo,
      final Map<String, List<String>> snapshotFiles) throws IOException {
    Path regionDir = new Path(tableDir, regionInfo.getEncodedName());
    for (Map.Entry<String, List<String>> familyEntry: snapshotFiles.entrySet()) {
      Path familyDir = new Path(regionDir, familyEntry.getKey());
      for (String hfileName: familyEntry.getValue()) {
        HFileLink.create(conf, fs, familyDir, snapshotRegionInfo, hfileName);
      }
    }
  }

  /**
   * Create a new Region from the snapshot region info.
   *
   * @param snapshotRegionInfo Info for region to clone.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @param tableDescriptor
   * @return the new HRegion instance
   * @throws IOException
   */
  public static HRegion cloneRegion(final HRegionInfo snapshotRegionInfo, final Path rootDir,
      final Configuration conf, final HTableDescriptor tableDescriptor) throws IOException {
    HRegionInfo regionInfo = new HRegionInfo(tableDescriptor.getName(),
                      snapshotRegionInfo.getStartKey(), snapshotRegionInfo.getEndKey(),
                      snapshotRegionInfo.isSplit(), snapshotRegionInfo.getRegionId());
    return HRegion.createHRegion(regionInfo, rootDir, conf, tableDescriptor);
  }

  /**
   * Restore snapshot WALs.
   *
   * Global Snapshot keep a reference to region servers logs present during the snapshot.
   *    /hbase/.snapshot/<name>/.logs/<host>/<name>
   *
   * Since each log contains different tables data, logs must be splitted extract table data.
   */
  private void restoreWALs() throws IOException {
    Path path = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    FileStatus[] logDirs = FSUtils.listStatus(fs, path);
    if (logDirs == null) return;

    SnapshotLogSplitter logSplitter = new SnapshotLogSplitter(conf, fs, tableDir, 
                                Bytes.toBytes(snapshotDesc.getTable()), regionsMap);
    try {
      for (FileStatus serverLogs: logDirs) {
        FileStatus[] hlogs = FSUtils.listStatus(fs, serverLogs.getPath());
        if (hlogs == null) continue;

        for (FileStatus hlogRef: hlogs) {
          logSplitter.splitLog(hlogRef.getPath());
        }
      }
    } finally {
      logSplitter.close();
    }
  }

  /**
   * @return the set of the regions contained in the snapshot
   */
  private Set<String> getSnapshotRegionNames() throws IOException {
    FileStatus[] regionDirs = FSUtils.listStatus(fs, snapshotDir, new FSUtils.DirFilter(fs));
    if (regionDirs == null) return null;

    Set<String> regions = new HashSet<String>();
    for (FileStatus regionDir: regionDirs) {
      // Check if the directory contains a .regioninfo (Is a region?)
      Path regionInfoPath = new Path(regionDir.getPath(), HRegion.REGIONINFO_FILE);
      if (fs.exists(regionInfoPath)) regions.add(regionDir.getPath().getName());
    }
    return regions;
  }

  /**
   * @return the set of the regions contained in the table
   */
  private List<HRegionInfo> getTableRegions() throws IOException {
    LOG.debug("get table regions: " + tableDir);
    FileStatus[] regionDirs = FSUtils.listStatus(fs, tableDir, new FSUtils.DirFilter(fs));
    if (regionDirs == null) return null;

    List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
    for (FileStatus regionDir: regionDirs) {
      try {
        HRegionInfo hri = HRegion.loadDotRegionInfoFileContent(fs, regionDir.getPath());
        regions.add(hri);
      } catch (FileNotFoundException e) {
        // Not a region, skip it.
      }
    }
    LOG.debug("found " + regions.size() + " regions for table=" + tableDesc.getNameAsString());
    return regions;
  }

  /**
   * Create a new table cloning the snapshot table schema.
   *
   * @param admin
   * @param snapshotTableDescriptor
   * @param tableName
   * @return restored table descriptor
   * @throws IOException
   */
  public static HTableDescriptor cloneTableSchema(final HBaseAdmin admin,
      final HTableDescriptor snapshotTableDescriptor, final byte[] tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (HColumnDescriptor hcd: snapshotTableDescriptor.getColumnFamilies()) {
      htd.addFamily(hcd);
    }
    admin.createTable(htd);
    return htd;
  }

  /**
   * Restore snapshot table schema on specified table.
   *
   * @param admin
   * @param snapshotTableDescriptor
   * @param tableName
   * @return restored table descriptor
   * @throws IOException
   */
  public static HTableDescriptor restoreTableSchema(final HBaseAdmin admin,
      final HTableDescriptor snapshotTableDescriptor,
      final byte[] tableName) throws IOException {
    HTableDescriptor tableDesc = admin.getTableDescriptor(tableName);
    Set<byte[]> snapshotFamilies = snapshotTableDescriptor.getFamiliesKeys();
    Set<byte[]> tableFamilies = tableDesc.getFamiliesKeys();

    // Remove columns that are not in the snapshot
    for (byte[] familyName: tableFamilies) {
      if (!snapshotFamilies.contains(familyName)) {
        LOG.info("Delete table=" + tableDesc.getNameAsString() +
                 " column=" + Bytes.toString(familyName));
        admin.deleteColumn(tableName, familyName);
      }
    }

    // Update or add columns in the snapshot
    for (HColumnDescriptor hcd: snapshotTableDescriptor.getColumnFamilies()) {
      byte[] familyName = hcd.getName();
      if (tableFamilies.contains(familyName)) {
        LOG.info("modify table=" + tableDesc.getNameAsString() + " column=" + hcd);
        admin.modifyColumn(tableName, hcd);
      } else {
        LOG.info("add table=" + tableDesc.getNameAsString() + " column=" + hcd);
        admin.addColumn(tableName, hcd);
      }
    }

    return admin.getTableDescriptor(tableName);
  }
}
