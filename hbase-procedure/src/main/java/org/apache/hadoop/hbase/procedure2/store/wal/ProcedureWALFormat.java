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

package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.procedure2.util.ByteSlot;
import org.apache.hadoop.hbase.procedure2.util.CodingUtil;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALHeader;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALTrailer;

/**
 * Helper class that contains the WAL serialization utils.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureWALFormat {
  static final byte TXN_TYPE_INIT    = 0;
  static final byte TXN_TYPE_INSERT  = 1;
  static final byte TXN_TYPE_UPDATE  = 2;
  static final byte TXN_TYPE_DELETE  = 3;
  static final byte TXN_TYPE_COMPACT = 4;
  static final byte TXN_TYPE_EOF = 7;

  static final byte LOG_TYPE_STREAM = 0;
  static final byte LOG_TYPE_COMPACTED = 1;
  static final byte LOG_TYPE_MAX_VALID = 1;

  static final byte HEADER_VERSION = 1;
  static final byte TRAILER_VERSION = 1;
  static final long HEADER_MAGIC = 0x31764c4157637250L;
  static final long TRAILER_MAGIC = 0x50726357414c7631L;

  interface Loader {
    void removeLog(ProcedureWALFile log);
    void markCorruptedWAL(ProcedureWALFile log, IOException e);
    Procedure deserializeProcedure(long procId, byte[] data) throws IOException;
  }

  public static Iterator<Procedure> load(final Iterator<ProcedureWALFile> logs,
      final ProcedureStoreTracker tracker, final Loader loader) throws IOException {
    ProcedureWALFormatReader reader = new ProcedureWALFormatReader(tracker);
    ProcedureWALFile log = logs.hasNext() ? logs.next() : null;
    while (log != null) {
      ProcedureWALFile nextLog = logs.hasNext() ? logs.next() : null;
      log.open();
      try {
        reader.read(log, loader);
      } finally {
        log.close();
      }
      log = nextLog;
    }
    // TODO: Write compacted version?
    return reader.getProcedures();
  }

  public static void writeHeader(OutputStream stream, ProcedureWALHeader header)
      throws IOException {
    header.writeDelimitedTo(stream);
  }

  /*
   * +---------------+
   * | EOF --- --    | <---+
   * +---------------+     |
   * |               |     |
   * |    Tracker    |     |
   * |               |     |
   * +---------------+     |
   * |    version    |     |
   * +---------------+     |
   * | TRAILER_MAGIC |     |
   * +---------------+     |
   * |     offset    |-----+
   * +---------------+
   */
  public static void writeTrailer(FSDataOutputStream stream, ProcedureStoreTracker tracker)
      throws IOException {
    long offset = stream.getPos();

    stream.write(TXN_TYPE_EOF << 5);
    tracker.writeTo(stream);

    stream.write(TRAILER_VERSION);
    CodingUtil.writeInt(stream, TRAILER_MAGIC, 8);
    CodingUtil.writeInt(stream, offset, 8);
  }

  public static void writeInsert(ByteSlot slot, Procedure proc)
      throws IOException {
    int procIdLength = CodingUtil.getIntSize(proc.getProcId());
    int sizeLength = CodingUtil.getIntSize(slot.size());
    int size = slot.size();

    // Write Header
    slot.markHead();
    slot.write((TXN_TYPE_INIT << 5) | ((procIdLength - 1) << 2) | (sizeLength - 1));
    CodingUtil.writeInt(slot, proc.getProcId(), procIdLength);
    CodingUtil.writeInt(slot, size, sizeLength);
  }

  public static void writeInsert(ByteSlot slot, Procedure proc, Procedure[] subprocs, int[] sizes)
      throws IOException {
    int subProcLenSize = CodingUtil.getIntSize(subprocs.length);

    // Write Header
    slot.markHead();
    slot.write((TXN_TYPE_INSERT << 5) | (subProcLenSize - 1));
    CodingUtil.writeInt(slot, subprocs.length, subProcLenSize);
    writeTxnIds(slot, proc, subprocs);
    CodingUtil.packInts(slot, sizes, sizes.length);
  }

  public static void writeUpdate(ByteSlot slot, Procedure proc)
      throws IOException {
    int procIdLength = CodingUtil.getIntSize(proc.getProcId());
    int sizeLength = CodingUtil.getIntSize(slot.size());
    int size = slot.size();

    // Write Header
    slot.markHead();
    slot.write((TXN_TYPE_UPDATE << 5) | ((procIdLength - 1) << 2) | (sizeLength - 1));
    CodingUtil.writeInt(slot, proc.getProcId(), procIdLength);
    CodingUtil.writeInt(slot, size, sizeLength);
  }

  public static void writeDelete(ByteSlot slot, long procId)
      throws IOException {
    int procIdLength = CodingUtil.getIntSize(procId);

    // Write Header
    slot.markHead();
    slot.write((TXN_TYPE_DELETE << 5) | ((procIdLength - 1) << 2));
    CodingUtil.writeInt(slot, procId, procIdLength);
  }

  public static void writeCompaction(OutputStream stream, long procId, byte[] data)
      throws IOException {
    int procIdLength = CodingUtil.getIntSize(procId);
    int sizeLength = CodingUtil.getIntSize(data.length);

    // Write Header
    stream.write((TXN_TYPE_COMPACT << 5) | ((procIdLength - 1) << 2) | (sizeLength - 1));
    CodingUtil.writeInt(stream, procId, procIdLength);
    CodingUtil.writeInt(stream, data.length, sizeLength);
    stream.write(data);
  }

  public static long readProcId(final InputStream stream, final int head)
      throws IOException {
    return CodingUtil.readLong(stream, 1 + ((head >> 2) & 7));
  }

  public static int readSize(final InputStream stream, final int head)
      throws IOException {
    return CodingUtil.readInt(stream, 1 + (head & 3));
  }

  public static int readType(final int head) {
    return (head >> 5) & 7;
  }

  public static int readNumProcedures(final InputStream stream, final int head)
      throws IOException {
    return 1 + readSize(stream, head);
  }

  public static void writeTxnIds(OutputStream stream, Procedure proc, Procedure[] subprocs)
      throws IOException {
    long[] procIds = new long[1 + subprocs.length];
    procIds[0] = proc.getProcId();
    for (int i = 0; i < subprocs.length; ++i) {
      procIds[i+1] = subprocs[i].getProcId() - procIds[0];
    }
    CodingUtil.packLongs(stream, procIds, procIds.length);
  }

  public static void readTxnIds(InputStream stream, long[] procIds, int numProcs)
      throws IOException {
    CodingUtil.unpackLongs(stream, procIds, numProcs);
    for (int i = 1; i < numProcs; ++i) {
      procIds[i] += procIds[0];
    }
  }

  public static void readTxnSizes(InputStream stream, int[] procSizes, int numProcs)
      throws IOException {
    CodingUtil.unpackInts(stream, procSizes, numProcs);
  }

  public static ProcedureWALHeader readHeader(InputStream stream)
      throws IOException {
    ProcedureWALHeader header = ProcedureWALHeader.parseDelimitedFrom(stream);

    if (header.getVersion() < 0 || header.getVersion() != HEADER_VERSION) {
      throw new IOException("Invalid Header version. got " + header.getVersion() +
          " expected " + HEADER_VERSION);
    }

    if (header.getType() < 0 || header.getType() > LOG_TYPE_MAX_VALID) {
      throw new IOException("Invalid header type. got " + header.getType());
    }

    return header;
  }

  public static ProcedureWALTrailer readTrailer(FSDataInputStream stream, long startPos, long size)
      throws IOException {
    long trailerPos = size - 17; // Beginning of the Trailer Jump

    if (trailerPos < startPos) {
      throw new IOException("Missing trailer: size=" + size + " startPos=" + startPos);
    }

    stream.seek(trailerPos);
    int version = stream.read();
    if (version != TRAILER_VERSION) {
      throw new IOException("Invalid Trailer version. got " + version +
          " expected " + TRAILER_VERSION);
    }

    long magic = CodingUtil.readLong(stream, 8);
    if (magic != TRAILER_MAGIC) {
      throw new IOException("Invalid Trailer magic. got " + magic +
          " expected " + TRAILER_MAGIC);
    }

    long trailerOffset = CodingUtil.readLong(stream, 8);
    stream.seek(trailerOffset);

    if (stream.read() != (TXN_TYPE_EOF << 5)) {
      throw new IOException("Invalid Trailer begin");
    }

    ProcedureWALTrailer trailer = ProcedureWALTrailer.newBuilder()
      .setVersion(version)
      .setTrackerPos(stream.getPos())
      .build();
    return trailer;
  }
}