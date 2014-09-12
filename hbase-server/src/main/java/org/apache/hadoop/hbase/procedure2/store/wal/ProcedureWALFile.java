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

package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.procedure2.ProcedureStoreTracker;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureWALFile implements Comparable<ProcedureWALFile> {
  private ProcedureWALHeader header;
  private FSDataInputStream stream;
  private FileStatus logStatus;
  private FileSystem fs;
  private Path logFile;
  private long startPos;

  public ProcedureWALFile(final FileSystem fs, final FileStatus logStatus) {
    this.fs = fs;
    this.logStatus = logStatus;
    this.logFile = logStatus.getPath();
    System.out.println("logFile=" + logFile + " size=" + logStatus.getLen());
  }

  public ProcedureWALFile(FileSystem fs, Path logFile, ProcedureWALHeader header, long startPos) {
    this.fs = fs;
    this.logFile = logFile;
    this.header = header;
    this.startPos = startPos;
  }

  public void open() throws IOException {
    if (stream == null) {
      stream = fs.open(logFile);
    }

    if (header == null) {
      header = ProcedureWALFormat.readHeader(stream);
      startPos = stream.getPos();
    } else {
      stream.seek(startPos);
    }
  }

  public ProcedureWALTrailer readTrailer() throws IOException {
    try {
      return ProcedureWALFormat.readTrailer(stream, startPos, logStatus.getLen());
    } finally {
      stream.seek(startPos);
    }
  }

  public void readTracker(ProcedureStoreTracker tracker) throws IOException {
    ProcedureWALTrailer trailer = readTrailer();
    try {
      stream.seek(trailer.getTrackerPos());
      tracker.readFrom(stream);
    } finally {
      stream.seek(startPos);
    }
  }

  public void close() {
    if (stream == null) return;
    try {
      stream.close();
    } catch (IOException e) {
      // TODO
    } finally {
      stream = null;
    }
  }

  public FSDataInputStream getStream() {
    return stream;
  }

  public ProcedureWALHeader getHeader() {
    return header;
  }

  public boolean isCompacted() {
    return header.getType() == ProcedureWALFormat.LOG_TYPE_COMPACTED;
  }

  public long getLogId() {
    return header.getLogId();
  }

  public long getSize() {
    return logStatus.getLen();
  }

  public void removeFile() throws IOException {
    close();
    fs.delete(logFile, false);
  }

  @Override
  public int compareTo(final ProcedureWALFile other) {
    long diff = header.getLogId() - other.header.getLogId();
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
  }

  @Override
  public String toString() {
    return logFile.toString();
  }
}