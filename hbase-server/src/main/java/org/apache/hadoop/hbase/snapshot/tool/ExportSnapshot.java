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

package org.apache.hadoop.hbase.snapshot.tool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Export the specified snapshot to a given FileSystem.
 *
 * The .snapshot/name folder is copied to the destination cluster
 * and then all the hfiles/hlogs are copied using a Map-Reduce Job in the .archive/ location.
 * When everything is done, the second cluster can restore the snapshot.
 */
public final class ExportSnapshot extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(ExportSnapshot.class);

  private final String INPUT_FOLDER_PREFIX = "export-files.";
  private final int MAX_FILE_PER_MAPPER = 500;
  private final int MIN_FILE_PER_MAPPER = 10;

  public enum Counter { MISSING_FILES, COPY_FAILED, BYTES_EXPECTED, BYTES_COPIED };

  public static class ExportMapper extends Mapper<Text, NullWritable, NullWritable, NullWritable> {
    final static int BUFFER_SIZE = 64 << 10;
    final static int REPORT_SIZE = 1 << 20;

    private boolean verifyChecksum;

    private FileSystem outputFs;
    private Path outputArchive;
    private Path outputRoot;

    private FileSystem inputFs;
    private Path inputArchive;
    private Path inputRoot;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      verifyChecksum = conf.getBoolean("snapshot.export.checksum.verify", true);
      outputArchive = new Path(conf.get("snapshot.export.output.archive"));
      outputRoot = new Path(conf.get("snapshot.export.output.root"));
      inputArchive = new Path(conf.get("snapshot.export.input.archive"));
      inputRoot = new Path(conf.get("snapshot.export.input.root"));

      try {
        inputFs = FileSystem.get(inputRoot.toUri(), conf);
      } catch (IOException e) {
        throw new RuntimeException("Could not get the input FileSystem", e);
      }

      try {
        outputFs = FileSystem.get(outputRoot.toUri(), conf);
      } catch (IOException e) {
        throw new RuntimeException("Could not get the output FileSystem", e);
      }
    }

    public void cleanup(Context context) {
      if (outputFs == null) {
        try {
          outputFs.close();
        } catch (IOException e) {
          LOG.error("Error closing output FileSystem", e);
        }
      }

      if (inputFs == null) {
        try {
          inputFs.close();
        } catch (IOException e) {
          LOG.error("Error closing input FileSystem", e);
        }
      }
    }

    public void map(Text key, NullWritable value, Context context)
        throws InterruptedException, IOException {
      Path inputPath = new Path(key.toString());
      Path outputPath = getOutputPath(inputPath);

      LOG.info("copy file input=" + inputPath + " output=" + outputPath);
      if (copyFile(context, inputPath, outputPath)) {
        LOG.info("copy completed for input=" + inputPath + " output=" + outputPath);
      }
    }

    private Path getOutputPath(final Path inputPath) {
      Path path;
      if (HFileLink.isHFileLink(inputPath)) {
        String family = inputPath.getParent().getName();
        String table = HFileLink.getReferencedTableName(inputPath.getName());
        String region = HFileLink.getReferencedRegionName(inputPath.getName());
        String hfile = HFileLink.getReferencedHFileName(inputPath.getName());
        path = new Path(table, new Path(region, new Path(family, hfile)));
      } else {
        path = inputPath;
      }
      return new Path(outputArchive, path);
    }

    private boolean copyFile(final Context context, final Path inputPath, final Path outputPath)
        throws IOException {
      FSDataInputStream in = openSourceFile(inputPath);
      if (in == null) {
        context.getCounter(Counter.MISSING_FILES).increment(1);
        return false;
      }

      try {
        FileStatus inputStat = getFileStatus(inputFs, inputPath);
        if (inputStat == null) return false;

        FileStatus outputStat = getFileStatus(outputFs, outputPath);
        if (outputStat != null && sameFile(inputStat, outputStat)) {
          LOG.info("Skip copy " + inputPath + " to " + outputPath + ", same file.");
          return true;
        }

        context.getCounter(Counter.BYTES_EXPECTED).increment(inputStat.getLen());

        outputFs.mkdirs(outputPath.getParent());
        FSDataOutputStream out = outputFs.create(outputPath, true);
        boolean success = copyData(context, inputPath, in, outputPath, out, inputStat.getLen());
        out.close();

        return success;
      } finally {
        in.close();
      }
    }

    private boolean copyData(final Context context,
        final Path inputPath, final FSDataInputStream in,
        final Path outputPath, final FSDataOutputStream out,
        final long inputFileSize) {
      final String statusMessage = "copied %s/" + StringUtils.humanReadableInt(inputFileSize) +
                                   " (%.3f%%) from " + inputPath + " to " + outputPath;

      try {
        byte[] buffer = new byte[BUFFER_SIZE];
        long totalBytesWritten = 0;
        int reportBytes = 0;
        int bytesRead;

        while ((bytesRead = in.read(buffer)) > 0) {
          out.write(buffer, 0, bytesRead);
          totalBytesWritten += bytesRead;
          reportBytes += bytesRead;

          if (reportBytes >= REPORT_SIZE) {
            context.getCounter(Counter.BYTES_COPIED).increment(reportBytes);
            context.setStatus(String.format(statusMessage,
                              StringUtils.humanReadableInt(totalBytesWritten),
                              reportBytes/(float)inputFileSize));
            reportBytes = 0;
          }
        }

        context.getCounter(Counter.BYTES_COPIED).increment(reportBytes);
        context.setStatus(String.format(statusMessage,
                          StringUtils.humanReadableInt(totalBytesWritten),
                          reportBytes/(float)inputFileSize));

        // Verify that the written size match
        if (totalBytesWritten != inputFileSize) {
          LOG.error("number of bytes copied not matching copied=" + totalBytesWritten +
                    " expected=" + inputFileSize + " for file=" + inputPath);
          context.getCounter(Counter.COPY_FAILED).increment(1);
          return false;
        }

        return true;
      } catch (IOException e) {
        LOG.error("Error copying " + inputPath + " to " + outputPath, e);
        context.getCounter(Counter.COPY_FAILED).increment(1);
        return false;
      }
    }

    private FSDataInputStream openSourceFile(final Path path) {
      try {
        if (HFileLink.isHFileLink(path)) {
          return new HFileLink(inputRoot, inputArchive, path).open(inputFs);
        }
        return inputFs.open(path);
      } catch (IOException e) {
        LOG.error("Unable to open source file=" + path, e);
        return null;
      }
    }

    private FileStatus getFileStatus(final FileSystem fs, final Path path) {
      try {
        if (HFileLink.isHFileLink(path)) {
          Path refPath = HFileLink.getReferencedPath(fs, inputRoot, inputArchive, path);
          return fs.getFileStatus(refPath);
        }
        return fs.getFileStatus(path);
      } catch (IOException e) {
        LOG.warn("Unable to get the status for file=" + path);
        return null;
      }
    }

    private FileChecksum getFileChecksum(final FileSystem fs, final Path path) {
      try {
        return fs.getFileChecksum(path);
      } catch (IOException e) {
        LOG.warn("Unable to get checksum for file=" + path, e);
        return null;
      }
    }

    private boolean sameFile(final FileStatus inputStat, final FileStatus outputStat) {
      // Not matching length
      if (inputStat.getLen() != outputStat.getLen()) return false;

      // Mark files as equals, since user asked for no checksum verification
      if (!verifyChecksum) return true;

      // If checksums are not available, files are not the same.
      FileChecksum inChecksum = getFileChecksum(inputFs, inputStat.getPath());
      if (inChecksum == null) return false;

      FileChecksum outChecksum = getFileChecksum(outputFs, outputStat.getPath());
      if (outChecksum == null) return false;

      return inChecksum.equals(outChecksum);
    }
  }

  /**
   * Extract the list of files (HFiles/HLogs) to copy using Map-Reduce.
   * The returned files are sorted by size.
   */
  private Path[] getSnapshotFiles(final FileSystem fs, final Path snapshotDir) throws IOException {
    List<Pair<Path, Long>> files = new ArrayList<Pair<Path, Long>>();

    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    HTableDescriptor tableDesc = FSTableDescriptors.getTableDescriptor(fs, snapshotDir,
                                                     Bytes.toBytes(snapshotDesc.getTable()));

    // Get regions hfiles
    String table = snapshotDesc.getTable();
    FileStatus[] regionDirs = FSUtils.listStatus(fs, snapshotDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs != null) {
      for (FileStatus region: regionDirs) {
        String regionName = region.getPath().getName();

        for (HColumnDescriptor hcd: tableDesc.getFamilies()) {
          String familyName = hcd.getNameAsString();

          FileStatus[] storeFiles = FSUtils.listStatus(fs, new Path(region.getPath(), familyName));
          if (storeFiles == null) continue;

          for (FileStatus hfile: storeFiles) {
            String hfileName = Reference.getDeferencedHFileName(hfile.getPath().getName());
            Path path = new Path(familyName, HFileLink.createHFileLinkName(table, regionName, hfileName));
            long size = fs.getFileStatus(HFileLink.getReferencedPath(getConf(), fs, path)).getLen();
            files.add(new Pair<Path, Long>(path, size));
          }
        }
      }
    }

    // TODO: Get logs
    // Maybe better having that already splitted or flushed before exporting
    Path logsDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    FileStatus[] logServerDirs = FSUtils.listStatus(fs, logsDir);
    if (logServerDirs != null) {
      for (FileStatus serverLogs: logServerDirs) {
        FileStatus[] hlogs = FSUtils.listStatus(fs, serverLogs.getPath());
        if (hlogs == null) continue;

        for (FileStatus hlogRef: hlogs) {
          // TODO: Resolve Logs HLogLink
        }
      }
    }

    // Sort files by size
    Collections.sort(files, new Comparator<Pair<Path, Long>>() {
      public int compare(Pair<Path, Long> a, Pair<Path, Long> b) {
        long r = a.getSecond() - b.getSecond();
        return (r < 0) ? 1 : (r > 0) ? -1 : 0;
      }
    });

    // Transform to path only
    Path[] snapshotFiles = new Path[files.size()];
    for (int i = 0; i < snapshotFiles.length; ++i) {
      LOG.debug(StringUtils.humanReadableInt(files.get(i).getSecond()) +
                " file=" + files.get(i).getFirst());
      snapshotFiles[i] = files.get(i).getFirst();
    }
    return snapshotFiles;
  }

  /**
   * Create the input files for the MR job.
   * Each input files contains n files, and each input file as a similar amount data to copy.
   */
  private Path[] createInputFiles(final Configuration conf,
      final Path[] snapshotFiles, int mappers) throws IOException, InterruptedException {
    Cluster cluster = new Cluster(conf);
    FileSystem fs = cluster.getFileSystem();
    Path stagingDir = JobSubmissionFiles.getStagingDir(cluster, conf);
    Path inputFolderPath = new Path(stagingDir, INPUT_FOLDER_PREFIX +
                                    String.valueOf(System.currentTimeMillis()));
    LOG.debug("Input folder location: " + inputFolderPath);

    int filesPerMapper = snapshotFiles.length / mappers;
    if (filesPerMapper > MAX_FILE_PER_MAPPER) {
      filesPerMapper = MAX_FILE_PER_MAPPER;
    } else if (filesPerMapper < MIN_FILE_PER_MAPPER) {
      filesPerMapper = MIN_FILE_PER_MAPPER;
    }

    Path[] inputFiles = new Path[(snapshotFiles.length / filesPerMapper) +
                                 ((snapshotFiles.length % filesPerMapper) == 0 ? 0 : 1)];
    LOG.debug("Number of files per mapper: " + filesPerMapper);
    LOG.debug("Number of input files: " + inputFiles.length);

    SequenceFile.Writer writer = null;
    try {
      Text key = new Text();
      for (int i = 0; i < inputFiles.length; ++i) {
        if (writer != null) writer.close();

        inputFiles[i] = new Path(inputFolderPath, String.format("export-%d.seq", i));
        writer = SequenceFile.createWriter(fs, conf, inputFiles[i], Text.class, NullWritable.class);

        for (int j = i; j < snapshotFiles.length; j += inputFiles.length) {
          key.set(snapshotFiles[j].toString());
          writer.append(key, NullWritable.get());
        }
      }
    } finally {
      if (writer != null) writer.close();
    }

    return inputFiles;
  }

  @Override
  public int run(String[] args) throws Exception {
    boolean verifyChecksum = true;
    byte[] snapshotName = null;
    Path outputArchive = null;
    Path outputRoot = null;
    int minMappers = getConf().getInt("mapreduce.job.maps", 1);

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      try {
        if (cmd.equals("-snapshot")) {
          snapshotName = Bytes.toBytes(args[++i]);
        } else if (cmd.equals("-copy-to")) {
          outputRoot = new Path(args[++i]);
        } else if (cmd.equals("-archive-dir")) {
          outputArchive = new Path(args[++i]);
        } else if (cmd.equals("-no-checksum-verify")) {
          verifyChecksum = false;
        } else if (cmd.equals("-min-mappers")) {
          minMappers = Integer.parseInt(args[++i]);
        } else if (cmd.equals("-h") || cmd.equals("--help")) {
          printUsageAndExit();
        } else {
          System.err.println("UNEXPECTED: " + cmd);
          printUsageAndExit();
        }
      } catch (Exception e) {
        printUsageAndExit();
      }
    }

    // Check user options
    if (snapshotName == null) {
      System.err.println("Snapshot name not provided.");
      printUsageAndExit();
    }

    if (outputRoot == null) {
      System.err.println("Destination file-system not provided.");
      printUsageAndExit();
    }

    String archiveDirName = HFileArchiveUtil.getConfiguredArchiveDirName(getConf());

    Path inputRoot = FSUtils.getRootDir(getConf());
    Path inputArchive = new Path(inputRoot, archiveDirName);
    if (outputArchive == null) {
      outputArchive = new Path(outputRoot, archiveDirName);
    } else {
      outputArchive = new Path(outputRoot, outputArchive);
    }

    FileSystem outputFs = FileSystem.get(outputRoot.toUri(), new Configuration());
    FileSystem inputFs = FileSystem.get(getConf());

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, inputRoot);
    Path snapshotTmpDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshotName, outputRoot);
    Path outputSnapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, outputRoot);
    if (outputFs.exists(outputSnapshotDir)) {
      System.err.println("The snapshot already exists in the destination.");
      return 1;
    }

    // Step 0 - Extract snapshot files to copy
    Path[] snapshotFiles = getSnapshotFiles(inputFs, snapshotDir);

    // Step 1 - Copy fs1:/.snapshot/<snapshot> to  fs2:/.snapshot/.tmp/<snapshot>
    try {
      FileUtil.copy(inputFs, snapshotDir, outputFs, snapshotTmpDir, false, false, getConf());
    } catch (IOException e) {
      System.err.println("Failed to copy the snapshot directory");
      System.err.println(e);
      return 1;
    }

    // Step 2 - Rename fs2:/.snapshot/.tmp/<snapshot> fs2:/.snapshot/<snapshot>
    outputFs.rename(snapshotTmpDir, outputSnapshotDir);

    // Step 3 - Start MR Job to copy files
    // The snapshot references must be copied before the files otherwise the files gets removed
    // by the HFileArchiver, since they have no references.
    try {
      Configuration conf = getConf();
      conf.setBoolean("snapshot.export.checksum.verify", verifyChecksum);
      conf.set("snapshot.export.output.root", outputRoot.toString());
      conf.set("snapshot.export.output.archive", outputArchive.toString());
      conf.set("snapshot.export.input.root", inputRoot.toString());
      conf.set("snapshot.export.input.archive", inputArchive.toString());
      conf.setInt("mapreduce.job.maps", minMappers);

      Job job = new Job(conf);
      job.setJobName("ExportSnapshot");
      job.setJarByClass(ExportSnapshot.class);
      job.setMapperClass(ExportMapper.class);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setMapSpeculativeExecution(false);
      job.setNumReduceTasks(0);
      for (Path path: createInputFiles(conf, snapshotFiles, minMappers)) {
        LOG.debug("Add Input Path=" + path);
        SequenceFileInputFormat.addInputPath(job, path);
      }

      if (!job.waitForCompletion(true)) {
        throw new Exception("Snapshot export failed!");
      }
      return 0;
    } catch (Exception e) {
      System.err.println("Snapshot export failed!");
      System.err.println(e);
      outputFs.delete(outputSnapshotDir, true);
      return 1;
    }
  }

  // ExportSnapshot
  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [options]\n", getClass().getName());
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help              Show this help and exit.");
    System.err.println("  -snapshot NAME        Snapshot to restore.");
    System.err.println("  -copy-to NAME         Remote destination hdfs://.");
    System.err.println("  -no-checksum-verify   Do not verify checksum.");
    System.err.println("");
    System.err.println("Examples:");
    System.err.println("");
    System.err.println("hbase " + getClass() + " -snapshot MySnapshot \\");
    System.err.println("  --copy-to hdfs:///srv2:8082/hbase");
    System.exit(1);
  }

  /**
   * The guts of the {@link #main} method.
   * Call this method to avoid the {@link #main(String[])} System.exit.
   * @param args
   * @return errCode
   * @throws Exception
   */
  static int innerMain(final String [] args) throws Exception {
    return ToolRunner.run(HBaseConfiguration.create(), new ExportSnapshot(), args);
  }

  public static void main(String[] args) throws Exception {
     System.exit(innerMain(args));
  }
}
