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

package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.FSUtils;

@InterfaceAudience.Private
public final class SnapshotReferenceUtil {
  public interface StoreFilesFilter {
    void storeFile (final String region, final String family, final String hfile)
      throws IOException;
  }

  public interface RecoveredEditsFilter {
    void recoveredEdits (final String region, final String logfile)
      throws IOException;
  }

  public interface LogFilesFilter {
    void logFile (final String server, final String logfile)
      throws IOException;
  }

  public interface FilesFilter extends StoreFilesFilter, RecoveredEditsFilter, LogFilesFilter {
  }

  private SnapshotReferenceUtil() {
    // private constructor for utility class
  }

  /**
   * Get the log directory for a specific snapshot
   * @param snapshotDir directory where the specific snapshot will be store
   * @param serverName name of the parent regionserver for the log files
   * @return path to the log home directory for the archive files.
   */
  public static Path getLogsDir(Path snapshotDir, String serverName) {
    return new Path(snapshotDir, HLogUtil.getHLogDirectoryName(serverName));
  }

  /**
   * Get the snapshot recovered.edits root dir
   */
  public static Path getRecoveredEditsDir(Path snapshotDir) {
    return new Path(snapshotDir, '.' + HLog.RECOVERED_EDITS_DIR);
  }

  /**
   * Get the snapshot recovered.edits dir for the specified region
   */
  public static Path getRecoveredEditsDir(Path snapshotDir, String regionName) {
    return new Path(getRecoveredEditsDir(snapshotDir), regionName);
  }

  /**
   * Get the snapshot recovered.edits file
   */
  public static Path getRecoveredEdits(Path snapshotDir, String regionName, String logfile) {
    return new Path(getRecoveredEditsDir(snapshotDir, regionName), logfile);
  }

  /**
   * Iterate over the snapshot store files, restored.edits and logs
   */
  public static void listReferencedFiles(final FileSystem fs, final Path snapshotDir,
      final FilesFilter filter) throws IOException {
    listStoreFiles(fs, snapshotDir, filter);
    listRecoveredEdits(fs, snapshotDir, filter);
    listLogFiles(fs, snapshotDir, filter);
  }

  /**
   * Iterate over the snapshot store files
   */
  public static void listStoreFiles(final FileSystem fs, final Path snapshotDir,
      final StoreFilesFilter filter) throws IOException {
    FileStatus[] regionDirs = FSUtils.listStatus(fs, snapshotDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs == null) return;

    for (FileStatus region: regionDirs) {
      Path regionDir = region.getPath();
      listStoreFiles(fs, regionDir, regionDir.getName(), filter);
    }
  }

  /**
   * Iterate over the snapshot store files in the specified region
   */
  public static void listStoreFiles(final FileSystem fs, final Path regionDir,
      final String regionName, final StoreFilesFilter filter) throws IOException {
    FileStatus[] familyDirs = FSUtils.listStatus(fs, regionDir, new FSUtils.FamilyDirFilter(fs));
    if (familyDirs == null) return;

    for (FileStatus family: familyDirs) {
      Path familyDir = family.getPath();
      listStoreFiles(fs, familyDir, regionName, familyDir.getName(), filter);
    }
  }

  /**
   * Iterate over the snapshot store files in the specified region/family
   */
  public static void listStoreFiles(final FileSystem fs, final Path familyDir,
      final String regionName, final String familyName, final StoreFilesFilter filter)
      throws IOException {
    FileStatus[] storeFiles = FSUtils.listStatus(fs, familyDir);
    if (storeFiles == null) return;

    for (FileStatus hfile: storeFiles) {
      String hfileName = Reference.getDeferencedHFileName(hfile.getPath().getName());
      filter.storeFile(regionName, familyName, hfileName);
    }
  }

  /**
   * Iterate over the snapshot recovered.edits
   */
  public static void listRecoveredEdits(final FileSystem fs, final Path snapshotDir,
      final RecoveredEditsFilter filter) throws IOException {
    Path editsDir = getRecoveredEditsDir(snapshotDir);
    FileStatus[] logServerDirs = FSUtils.listStatus(fs, editsDir);
    if (logServerDirs == null) return;

    for (FileStatus serverLogs: logServerDirs) {
      FileStatus[] hlogs = FSUtils.listStatus(fs, serverLogs.getPath());
      if (hlogs == null) continue;

      for (FileStatus hlogRef: hlogs) {
        Path hlogRefPath = hlogRef.getPath();
        String logName = hlogRefPath.getName();
        String regionName = hlogRefPath.getParent().getName();
        filter.recoveredEdits(regionName, logName);
      }
    }
  }

  /**
   * Iterate over the snapshot recovered.edits and log files
   */
  public static void listLogFiles(final FileSystem fs, final Path snapshotDir,
      final LogFilesFilter filter) throws IOException {
    Path logsDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    FileStatus[] logServerDirs = FSUtils.listStatus(fs, logsDir);
    if (logServerDirs == null) return;

    for (FileStatus serverLogs: logServerDirs) {
      FileStatus[] hlogs = FSUtils.listStatus(fs, serverLogs.getPath());
      if (hlogs == null) continue;

      for (FileStatus hlogRef: hlogs) {
        Path hlogRefPath = hlogRef.getPath();
        String logName = hlogRefPath.getName();
        String serverName = hlogRefPath.getParent().getName();
        filter.logFile(serverName, logName);
      }
    }
  }

  /**
   * @return the set of the regions contained in the snapshot
   */
  public static Set<String> getSnapshotRegionNames(final FileSystem fs, final Path snapshotDir)
      throws IOException {
    FileStatus[] regionDirs = FSUtils.listStatus(fs, snapshotDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs == null) return null;

    Set<String> regions = new HashSet<String>();
    for (FileStatus regionDir: regionDirs) {
      regions.add(regionDir.getPath().getName());
    }
    return regions;
  }

  /**
   * Get the list of hfiles for the specified snapshot region.
   * NOTE: The current implementation keep one Reference file per HFile in a region folder.
   *
   * @param snapshotRegionInfo Snapshot region info
   * @throws IOException
   */
  public static Map<String, List<String>> getRegionHFileReferences(final FileSystem fs,
      final Path snapshotRegionDir) throws IOException {
    final Map<String, List<String>> familyFiles = new TreeMap<String, List<String>>();

    SnapshotReferenceUtil.listStoreFiles(fs, snapshotRegionDir, snapshotRegionDir.getName(),
      new SnapshotReferenceUtil.StoreFilesFilter() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          List<String> hfiles = familyFiles.get(family);
          if (hfiles == null) {
            hfiles = new LinkedList<String>();
            familyFiles.put(family, hfiles);
          }
          hfiles.add(hfile);
        }
    });

    return familyFiles;
  }
}
