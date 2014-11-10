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
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.procedure2.util.CodingUtil;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALHeader;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALTrailer;

/**
 * Helper class that loads the procedures stored in a WAL
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureWALFormatReader {
  private static final Log LOG = LogFactory.getLog(ProcedureWALFormatReader.class);

  private final ProcedureStoreTracker tracker;
  //private final long compactionLogId;

  private final Map<Long, Procedure> procedures = new HashMap<Long, Procedure>();
  private final Map<Long, byte[]> localProcedures = new HashMap<Long, byte[]>();

  private int maxTxnProcs = 0;
  private byte[][] txnBlocks;
  private long[] txnProcIds;
  private int[] txnSizes;

  public ProcedureWALFormatReader(final ProcedureStoreTracker tracker) {
    this.tracker = tracker;
  }

  public void read(ProcedureWALFile log, ProcedureWALFormat.Loader loader) throws IOException {
      FSDataInputStream stream = log.getStream();

      try {
        ProcedureWALTrailer trailer = log.readTrailer();
      } catch (IOException e) {
        LOG.warn("Unable to load trailer of " + log, e);
      }

    try {
      boolean hasMore = true;
      while (hasMore) {
        int head = stream.read();
        if (head < 0) break;

        int type = ProcedureWALFormat.readType(head);
        switch (type) {
          case ProcedureWALFormat.TXN_TYPE_INIT:
            readInitEntry(stream, head);
            break;
          case ProcedureWALFormat.TXN_TYPE_INSERT:
            readInsertEntry(stream, head);
            break;
          case ProcedureWALFormat.TXN_TYPE_UPDATE:
          case ProcedureWALFormat.TXN_TYPE_COMPACT:
            readUpdateEntry(stream, head);
            break;
          case ProcedureWALFormat.TXN_TYPE_DELETE:
            readDeleteEntry(stream, head);
            break;
          case ProcedureWALFormat.TXN_TYPE_EOF:
            hasMore = false;
            break;
          default:
            throw new CorruptedProcedureWALException("Invalid entry type: " + type);
        }
      }
    } catch (IOException e) {
      LOG.error("got an exception while reading the procedure WAL: " + log, e);
      loader.markCorruptedWAL(log, e);
    }

    if (localProcedures.isEmpty()) {
      LOG.info("No active entry found in state log " + log + ". removing it");
      loader.removeLog(log);
    } else {
      Iterator<Map.Entry<Long, byte[]>> itd = localProcedures.entrySet().iterator();
      while (itd.hasNext()) {
        Map.Entry<Long, byte[]> entry = itd.next();
        itd.remove();

        // Deserialize the procedure
        Procedure proc = loader.deserializeProcedure(entry.getKey(), entry.getValue());
        procedures.put(entry.getKey(), proc);
      }

      // TODO: Some procedure may be already runnables (see readInitEntry())
    }
  }

  public Iterator<Procedure> getProcedures() {
    return procedures.values().iterator();
  }

  private void readInitEntry(InputStream stream, final int head) throws IOException {
    long procId = ProcedureWALFormat.readProcId(stream, head);
    int size = ProcedureWALFormat.readSize(stream, head);
    if (isRequired(procId)) {
      LOG.trace("read init entry " + procId + " size=" + size);
      byte[] data = readProcData(stream, size);
      localProcedures.put(procId, data);
      tracker.setDeleted(procId, false);
      // TODO: Make it runnable, before reading other files
    } else {
      skipProcData(stream, size);
    }
  }

  private void readInsertEntry(InputStream stream, final int head) throws IOException {
    int numProcs = ProcedureWALFormat.readNumProcedures(stream, head);
    assert numProcs > 0;

    if (numProcs > maxTxnProcs) {
      maxTxnProcs = (numProcs + 15) & -16;
      txnBlocks = new byte[numProcs][];
      txnProcIds = new long[numProcs];
      txnSizes = new int[numProcs];
    }

    ProcedureWALFormat.readTxnIds(stream, txnProcIds, numProcs);
    ProcedureWALFormat.readTxnSizes(stream, txnSizes, numProcs);
    // TODO: validate entries
    for (int i = 0; i < numProcs; ++i) {
      if (isRequired(txnProcIds[i])) {
        LOG.trace("read insert entry " + txnProcIds[i] + " size=" + txnSizes[i]);
        txnBlocks[i] = readProcData(stream, txnSizes[i]);
        localProcedures.put(txnProcIds[i], txnBlocks[i]);
        tracker.setDeleted(txnProcIds[i], false);
      } else {
        skipProcData(stream, txnSizes[i]);
      }
    }
  }

  private void readUpdateEntry(InputStream stream, final int head) throws IOException {
    long procId = ProcedureWALFormat.readProcId(stream, head);
    int size = ProcedureWALFormat.readSize(stream, head);
    if (isRequired(procId)) {
      LOG.trace("read update entry " + procId + " size=" + size);
      byte[] data = readProcData(stream, size);
      localProcedures.put(procId, data);
      tracker.setDeleted(procId, false);
    } else {
      skipProcData(stream, size);
    }
  }

  private void readDeleteEntry(InputStream stream, final int head) throws IOException {
    long procId = ProcedureWALFormat.readProcId(stream, head);
    LOG.trace("read delete entry " + procId);
    localProcedures.remove(procId);
    tracker.setDeleted(procId, true);
  }

  private byte[] readProcData(InputStream stream, int size) throws IOException {
    byte[] block = new byte[size];
    int rdsize = stream.read(block, 0, size);
    if (rdsize != size) {
      throw new CorruptedProcedureWALException("Unable to read the full block: expected=" +
        size + " got=" + rdsize);
    }
    return block;
  }

  private void skipProcData(InputStream stream, int size) throws IOException {
    long n = stream.skip(size);
    if (n != size) {
      // TODO
      throw new CorruptedProcedureWALException("unable to skip " + size + " got " + n);
    }
  }

  private boolean isDeleted(final long procId) {
    return tracker.isDeleted(procId);
  }

  private boolean isRequired(final long procId) {
    return !isDeleted(procId) && !procedures.containsKey(procId);
  }
}