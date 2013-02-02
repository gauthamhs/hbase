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

package org.apache.hadoop.hbase.regionserver;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class HRegionFileSystem {
  public static final Log LOG = LogFactory.getLog(HRegionFileSystem.class);

  /** Name of the region info file that resides just under the region directory. */
  public final static String REGION_INFO_FILE = ".regioninfo";

  /** Temporary subdirectory of the region directory used for merges. */
  public static final String REGION_MERGES_DIR = ".merges";

  /** Temporary subdirectory of the region directory used for splits. */
  public static final String REGION_SPLITS_DIR = ".splits";

  /** Temporary subdirectory of the region directory used for compaction output. */
  private static final String REGION_TEMP_DIR = ".tmp";

  private final HRegionInfo regionInfo;
  private final Configuration conf;
  private final Path tableDir;
  private final FileSystem fs;

  public HRegionFileSystem(final Configuration conf, final FileSystem fs,
      final Path tableDir, final HRegionInfo regionInfo) {
    this.fs = fs;
    this.conf = conf;
    this.tableDir = tableDir;
    this.regionInfo = regionInfo;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public HRegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  public Path getTableDir() {
    return this.tableDir;
  }

  public Path getRegionDir() {
    return new Path(this.tableDir, this.regionInfo.getEncodedName());
  }

  public Path getTempDir() {
    return new Path(getRegionDir(), REGION_TEMP_DIR);
  }

  public Path getSplitsDir() {
    return new Path(getRegionDir(), REGION_SPLITS_DIR);
  }

  public Path getMergesDir() {
    return new Path(getRegionDir(), REGION_MERGES_DIR);
  }

  private Path getRegionInfoFile() {
    return new Path(getRegionDir(), REGION_INFO_FILE);
  }

  public Path getStoreDir(final String familyName) {
    return new Path(getRegionDir(), familyName);
  }

  public void cleanupTempDir() throws IOException {
    FSUtils.deleteDirectory(fs, getTempDir());
  }

  public void cleanupMergesDir() throws IOException {
    FSUtils.deleteDirectory(fs, getMergesDir());
  }

  public void cleanupSplitsDir() throws IOException {
    FSUtils.deleteDirectory(fs, getSplitsDir());
  }

  //////////////////////////////////////////////////////////////////////////////
  // Store Files
  //////////////////////////////////////////////////////////////////////////////
  public Path createTempName() {
    return createTempName(null);
  }

  public Path createTempName(final String suffix) {
    return new Path(getTempDir(), generateRandomName(suffix));
  }

  private String generateRandomName(final String suffix) {
    String name = UUID.randomUUID().toString().replaceAll("-", "");
    if (suffix != null) name += suffix;
    return name;
  }

  public Path commitStoreFile(final String familyName, final Path buildPath) throws IOException {
    return commitStoreFile(familyName, buildPath, -1);
  }

  public Path commitStoreFile(final String familyName, final Path buildPath, final long seqNum)
      throws IOException {
    String name = generateRandomName((seqNum == -1) ? null : "_SeqId_" + seqNum + "_");
    Path dstPath = new Path(getStoreDir(familyName), name);
    if (!fs.exists(buildPath)) {
      throw new FileNotFoundException(buildPath.toString());
    }
    LOG.debug("Committing store file " + buildPath + " as " + dstPath);
    if (!fs.rename(buildPath, dstPath)) {
      throw new IOException("Failed rename of " + buildPath + " to " + dstPath);
    }
    return dstPath;
  }

  public Path bulkLoadStoreFile(final String familyName, Path srcPath, long seqNum)
      throws IOException {
    // Copy the file if it's on another filesystem
    FileSystem srcFs = srcPath.getFileSystem(conf);
    FileSystem desFs = fs instanceof HFileSystem ? ((HFileSystem)fs).getBackingFs() : fs;
    // We can't compare FileSystem instances as equals() includes UGI instance
    // as part of the comparison and won't work when doing SecureBulkLoad
    // TODO deal with viewFS
    if (!srcFs.getUri().equals(desFs.getUri())) {
      LOG.info("Bulk-load file " + srcPath + " is on different filesystem than " +
          "the destination store. Copying file over to destination filesystem.");
      Path tmpPath = createTempName();
      FileUtil.copy(srcFs, srcPath, fs, tmpPath, false, conf);
      LOG.info("Copied " + srcPath + " to temporary path on destination filesystem: " + tmpPath);
      srcPath = tmpPath;
    }
    return commitStoreFile(familyName, srcPath, seqNum);
  }

  public static class StoreFileInfo {
    private final FileStatus fileStatus;

    public StoreFileInfo(final HRegionFileSystem fs, final FileStatus fileStatus) {
      this.fileStatus = fileStatus;
    }

    public static boolean isValid(final HRegionFileSystem fs, final FileStatus fileStatus)
        throws IOException {
      final Path p = fileStatus.getPath();

      if (fileStatus.isDir())
        return false;

      // Check for empty hfile. Should never be the case but can happen
      // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
      // NOTE: that the HFileLink is just a name, so it's an empty file.
      if (!HFileLink.isHFileLink(p) && fileStatus.getLen() <= 0) {
        LOG.warn("Skipping " + p + " beccreateStoreDirause its empty. HBASE-646 DATA LOSS?");
        return false;
      }

      return true;
    }

    public Path getPath() {
      return this.fileStatus.getPath();
    }

    public FileStatus getFileStatus() {
      return this.fileStatus;
    }

    private boolean isReference() {
      return false;
    }

    private boolean isLink() {
      return false;
    }
  }


  public Path createStoreDir(final String familyName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    if (!fs.exists(storeDir)) {
      if (!fs.mkdirs(storeDir))
        throw new IOException("Failed create of: " + storeDir);
    }
    return storeDir;
  }

  public Collection<StoreFileInfo> getStoreFiles(final byte[] familyName) throws IOException {
    return getStoreFiles(Bytes.toString(familyName));
  }

  public Collection<StoreFileInfo> getStoreFiles(final String familyName) throws IOException {
    Path familyDir = getStoreDir(familyName);
    FileStatus[] files = FSUtils.listStatus(this.fs, familyDir);
    if (files == null) return null;

    ArrayList<StoreFileInfo> storeFiles = new ArrayList<StoreFileInfo>(files.length);
    for (FileStatus status: files) {
      if (!StoreFileInfo.isValid(this, status)) continue;

      StoreFileInfo storeFileInfo = new StoreFileInfo(this, status);
      storeFiles.add(storeFileInfo);
    }
    return storeFiles;
  }

  public Collection<String> getFamilies() throws IOException {
    FileStatus[] fds = FSUtils.listStatus(fs, getRegionDir(), new FSUtils.FamilyDirFilter(fs));
    if (fds == null) return null;

    ArrayList<String> families = new ArrayList<String>(fds.length);
    for (FileStatus status: fds) {
      families.add(status.getPath().getName());
    }
    return families;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Create/Open Helpers
  //////////////////////////////////////////////////////////////////////////////

  /**
   * @param hri
   * @return Content of the file we write out to the filesystem under a region
   * @throws IOException
   */
  private static byte[] getRegionInfoFileContent(final HRegionInfo hri) throws IOException {
    return hri.toDelimitedByteArray();
  }

  /**
   * @param fs
   * @param regionInfoFile
   * @return An HRegionInfo instance gotten from the <code>.regioninfo</code> file under region dir
   * @throws IOException
   */
  public static HRegionInfo loadRegionInfoFileContent(final FileSystem fs,
      final Path regionInfoFile) throws IOException {
    FSDataInputStream in = fs.open(regionInfoFile);
    try {
      return HRegionInfo.parseFrom(in);
    } finally {
      in.close();
    }
  }

  private static void writeRegionInfoFileContent(final Configuration conf, final FileSystem fs,
      final Path regionInfoFile, final byte[] content) throws IOException {
    // First check to get the permissions
    FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // Write the RegionInfo file content
    FSDataOutputStream out = FSUtils.create(fs, regionInfoFile, perms);
    try {
      out.write(content);
    } finally {
      out.close();
    }
  }

  /**
   * Write out an info file under the stored region directory. Useful recovering mangled regions.
   * If the regioninfo already exists on disk and there is information in the file,
   * then we fast exit.
   * @throws IOException
   */
  void checkRegionInfoOnFilesystem() throws IOException {
    // Compose the content of the file so we can compare to length in filesystem. If not same,
    // rewrite it (it may have been written in the old format using Writables instead of pb). The
    // pb version is much shorter -- we write now w/o the toString version -- so checking length
    // only should be sufficient. I don't want to read the file every time to check if it pb
    // serialized.
    byte[] content = getRegionInfoFileContent(regionInfo);
    try {
      Path regionInfoFile = getRegionInfoFile();

      FileStatus status = fs.getFileStatus(regionInfoFile);
      if (status != null && status.getLen() == content.length) {
        // Then assume the content good and move on.
        return;
      }

      LOG.info("Rewriting .regioninfo file at " + regionInfoFile);
      if (!fs.delete(getRegionInfoFile(), false)) {
        throw new IOException("Unable to remove existing " + regionInfoFile);
      }
    } catch (FileNotFoundException e) {
      LOG.warn(REGION_INFO_FILE + " file not found for region: " + regionInfo.getEncodedName());
    }

    // Write HRI to a file in case we need to recover .META.
    writeRegionInfoOnFilesystem(content, true);
  }

  /**
   * Write out an info file under the region directory. Useful recovering mangled regions.
   * @param regionInfo information about the region
   * @param regionDir directory under which to write out the region info
   * @param fs {@link FileSystem} on which to write the region info
   * @param conf {@link Configuration} from which to extract specific file locations
   * @throws IOException on unexpected error.
   */
  private void writeRegionInfoOnFilesystem(boolean useTempDir) throws IOException {
    byte[] content = getRegionInfoFileContent(regionInfo);
    writeRegionInfoOnFilesystem(content, useTempDir);
  }

  private void writeRegionInfoOnFilesystem(final byte[] regionInfoContent,
      boolean useTempDir) throws IOException {
    if (useTempDir) {
      // Create in tmpdir and then move into place in case we crash after
      // create but before close. If we don't successfully close the file,
      // subsequent region reopens will fail the below because create is
      // registered in NN.

      // And then create the file
      Path tmpPath = new Path(getTempDir(), REGION_INFO_FILE);

      // If datanode crashes or if the RS goes down just before the close is called while trying to
      // close the created regioninfo file in the .tmp directory then on next
      // creation we will be getting AlreadyCreatedException.
      // Hence delete and create the file if exists.
      if (FSUtils.isExists(fs, tmpPath)) {
        FSUtils.delete(fs, tmpPath, true);
      }

      // Write HRI to a file in case we need to recover .META.
      writeRegionInfoFileContent(conf, fs, tmpPath, regionInfoContent);

      // Move the created file to the original path
      Path regionInfoFile = getRegionInfoFile();
      if (!fs.rename(tmpPath, regionInfoFile)) {
        throw new IOException("Unable to rename " + tmpPath + " to " + regionInfoFile);
      }
    } else {
      // Write HRI to a file in case we need to recover .META.
      writeRegionInfoFileContent(conf, fs, getRegionInfoFile(), regionInfoContent);
    }
  }

  /**
   * Create a new Region on file-system.
   */
  public static HRegionFileSystem createRegionOnFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    // Create the region directory
    if (!fs.mkdirs(regionFs.getRegionDir())) {
      throw new IOException("Unable to create region directory: " + regionFs.getRegionDir());
    }
    // Write HRI to a file in case we need to recover .META.
    regionFs.writeRegionInfoOnFilesystem(false);
    return regionFs;
  }

  /**
   * Open Region from file-system.
   * @param conf
   * @param fs
   * @param tableDir
   * @param regionInfo
   * @throw IOException
   */
  public static HRegionFileSystem openRegionFromFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    // Cleanup temporary directories
    regionFs.cleanupTempDir();
    regionFs.cleanupSplitsDir();
    regionFs.cleanupMergesDir();
    // Write HRI to a file in case we need to recover .META.
    regionFs.checkRegionInfoOnFilesystem();
    return regionFs;
  }

  public static void deleteRegionFromFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    Path regiondir = regionFs.getRegionDir();

    // TODO: Use the archiver
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETING region " + regiondir);
    }
    if (!fs.delete(regiondir, true)) {
      LOG.warn("Failed delete of " + regiondir);
    }
  }
}
