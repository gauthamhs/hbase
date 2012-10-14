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
package org.apache.hadoop.hbase.server.snapshot.task;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Reference all recover.edits for the region
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReferenceServerRecoverEditsTask extends SnapshotTask {
  private static final Log LOG = LogFactory.getLog(ReferenceServerRecoverEditsTask.class);

  private final FileSystem fs;
  private final Configuration conf;
  private final String regionName;
  private Path editsDir;

  /**
   * @param snapshot snapshot being run
   * @param failureListener listener to check for errors while running the operation and to
   *          propagate errors found while running the task
   * @param logDir log directory for the server. Name of the directory is taken as the name of the
   *          server
   * @param conf {@link Configuration} to extract fileystem information
   * @param fs filesystem where the log files are stored and should be referenced
   * @throws IOException
   */
  public ReferenceServerRecoverEditsTask(SnapshotDescription snapshot,
      SnapshotErrorListener failureListener, final Path regionDir,
      final Configuration conf, final FileSystem fs) throws IOException {
    super(snapshot, failureListener);
    this.fs = fs;
    this.conf = conf;
    this.regionName = regionDir.getName();
    this.editsDir = new Path(regionDir, HLog.RECOVERED_EDITS_DIR);
  }

  @Override
  public void run() {
    try {
      if (!fs.exists(editsDir)) return;

      this.failOnError();

      Path rootDir = FSUtils.getRootDir(conf);
      Path snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(this.snapshot, rootDir);
      Path snapshotLogDir = SnapshotReferenceUtil.getRecoveredEditsDir(snapshotDir, regionName);

      FileUtil.copy(fs, editsDir, fs, snapshotLogDir, false, false, conf);
      LOG.debug("Successfully completed recovered.edits referencing");
    } catch (HBaseSnapshotException e) {
      LOG.error("Could not complete adding recovered.edits files to snapshot "
          + "because received nofification that snapshot failed.");
    } catch (IOException e) {
      this.snapshotFailure("Error referencing recovered.edits files", e);
    }
  }
}