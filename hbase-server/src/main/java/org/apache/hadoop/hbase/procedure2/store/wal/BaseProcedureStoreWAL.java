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
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.procedure2.util.ByteSlot;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALHeader;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALTrailer;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class BaseProcedureStoreWAL implements ProcedureStore {
  private static final Log LOG = LogFactory.getLog(BaseProcedureStoreWAL.class);

  private static final int SYNC_WAIT_MSEC = 100;

  private final LinkedList<ProcedureWALFile> logs = new LinkedList<ProcedureWALFile>();
  private final ProcedureStoreTracker storeTracker = new ProcedureStoreTracker();
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();
  private final Condition slotCond = lock.newCondition();
  private final Condition syncCond = lock.newCondition();

  protected final FileSystem fs;

  private ArrayBlockingQueue<ByteSlot> slotsCache = null;
  private Set<ProcedureWALFile> corruptedLogs = null;
  private FSDataOutputStream stream = null;
  private boolean inSync = false;
  private long totalSynced = 0;
  private long flushLogId = 0;
  private int slotIndex = 0;
  private Thread syncThread;
  private ByteSlot[] slots;

  public BaseProcedureStoreWAL(final FileSystem fs) {
    this.fs = fs;
  }

  public void start(int numSlots) throws IOException {
    if (isRunning.getAndSet(true)) {
      return;
    }

    slots = new ByteSlot[numSlots];
    slotsCache = new ArrayBlockingQueue(numSlots, true);
    while (slotsCache.remainingCapacity() > 0) {
      slotsCache.offer(new ByteSlot());
    }

    flushLogId = initOldLogs();
    initSyncThread();

    // Keep a slot for a compacted logs if we have more than a log around
    long nextLogId = flushLogId + (logs.size() > 1 ? 2 : 1);
    if (!rollWriter(nextLogId)) {
      throw new IOException("Unable to create the state log");
    }
    syncThread.start();
  }

  public void stop() {
    isRunning.set(false);

    lock.lock();
    try {
      waitCond.signalAll();
    } finally {
      lock.unlock();
    }

    try {
      syncThread.join();
    } catch (InterruptedException e) {
      // TODO
    }

    // Close the writer
    closeStream();

    // Close the old logs
    // they should be already closed, this is just in case the load fails
    // and we call start() and then stop()
    for (ProcedureWALFile log: logs) {
      log.close();
    }
    logs.clear();
  }

  @Override
  public Iterator<Procedure> load() throws IOException {
    if (logs.size() == 1) {
      LOG.debug("No state logs to replay");
      return null;
    }

    Iterator<ProcedureWALFile> it = logs.descendingIterator();
    it.next();
    return ProcedureWALFormat.load(it, storeTracker, new ProcedureWALFormat.Loader() {
      @Override
      public void removeLog(ProcedureWALFile log) {
        removeLogFile(log);
      }

      @Override
      public void markCorruptedWAL(ProcedureWALFile log, IOException e) {
        if (corruptedLogs == null) {
          corruptedLogs = new HashSet<ProcedureWALFile>();
        }
        corruptedLogs.add(log);
        // TODO: sideline corrupted log
      }

      @Override
      public Procedure deserializeProcedure(long procId, byte[] data) throws IOException {
        return BaseProcedureStoreWAL.this.deserializeProcedure(procId, data);
      }
    });
  }

  @Override
  public void insert(final Procedure proc, final Procedure[] subprocs) {
    LOG.trace("insert " + proc + " subproc=" + subprocs);
    ByteSlot slot = acquireSlot();
    long logId = -1;
    try {
      // Serialize the insert
      if (subprocs != null) {
        int[] sizes = new int[1 + subprocs.length];
        serializeProcedure(slot, proc);
        sizes[0] = slot.size();
        int prevSize = slot.size();
        for (int i = 0; i < subprocs.length; ++i) {
          serializeProcedure(slot, subprocs[i]);
          sizes[i+1] = slot.size() - prevSize;
          prevSize = slot.size();
        }
        ProcedureWALFormat.writeInsert(slot, proc, subprocs, sizes);
      } else {
        assert !proc.hasParent();
        serializeProcedure(slot, proc);
        ProcedureWALFormat.writeInsert(slot, proc);
      }

      // Push the transaction data and wait until it is persisted
      logId = pushData(slot);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }

    // Update the store tracker
    synchronized (storeTracker) {
      if (logId == flushLogId) {
        storeTracker.insert(proc, subprocs);
      }
    }
  }

  @Override
  public void update(final Procedure proc) {
    LOG.trace("update " + proc);
    ByteSlot slot = acquireSlot();
    long logId = -1;
    try {
      // Serialize the update
      serializeProcedure(slot, proc);
      ProcedureWALFormat.writeUpdate(slot, proc);

      // Push the transaction data and wait until it is persisted
      logId = pushData(slot);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }

    // Update the store tracker
    boolean removeOldLogs = false;
    synchronized (storeTracker) {
      if (logId == flushLogId) {
        storeTracker.update(proc);
        removeOldLogs = storeTracker.isUpdated();
      }
    }

    if (removeOldLogs) {
      removeAllLogs(logId - 1);
    }
  }

  @Override
  public void delete(final long procId) {
    LOG.trace("delete " + procId);
    ByteSlot slot = acquireSlot();
    long logId = -1;
    try {
      // Serialize the delete
      ProcedureWALFormat.writeDelete(slot, procId);

      // Push the transaction data and wait until it is persisted
      logId = pushData(slot);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }

    boolean removeOldLogs = false;
    synchronized (storeTracker) {
      if (logId == flushLogId) {
        storeTracker.delete(procId);
        if (storeTracker.isEmpty()) {
          removeOldLogs = rollWriter(logId + 1);
        }
      }
    }

    if (removeOldLogs) {
      removeAllLogs(logId);
    }
  }

  private ByteSlot acquireSlot() {
    ByteSlot slot = slotsCache.poll();
    return slot != null ? slot : new ByteSlot();
  }

  private void releaseSlot(final ByteSlot slot) {
    slot.reset();
    slotsCache.offer(slot);
  }

  private long pushData(final ByteSlot slot) {
    long logId = -1;

    lock.lock();
    try {
      // Wait for the sync to be completed
      while (true) {
        if (inSync) {
          syncCond.await();
        } else if (slotIndex == slots.length) {
          slotCond.signal();
          syncCond.await();
        } else {
          break;
        }
      }

      slots[slotIndex++] = slot;
      logId = flushLogId;

      // Notify that there is new data
      if (slotIndex == 1) {
        waitCond.signal();
      }

      // Notify that the slots are full
      if (slotIndex == slots.length) {
        slotCond.signal();
      }
      syncCond.await();
    } catch (InterruptedException e) {
      // TODO
    } finally {
      lock.unlock();
    }
    return logId;
  }

  private long initOldLogs() throws IOException {
    FileStatus[] oldLogFiles = null;
    try {
      oldLogFiles = getLogFiles();
    } catch (FileNotFoundException e) {
      LOG.warn("log directory not found: " + e.getMessage());
    }

    if (oldLogFiles == null || oldLogFiles.length == 0) {
      return 0;
    }

    long maxCompactedLogId = 0;
    for (int i = 0; i < oldLogFiles.length; ++i) {
      ProcedureWALFile log = new ProcedureWALFile(fs, oldLogFiles[i]);
      try {
        log.open();
        LOG.debug("opening state-log: " + log);
        if (log.isCompacted()) {
          try {
            log.readTrailer();
          } catch (IOException e) {
            // unfinished compacted log throw it away
            LOG.warn("Unfinished compacted log " + oldLogFiles[i], e);
            log.removeFile();
            continue;
          }
          maxCompactedLogId = Math.max(maxCompactedLogId, log.getLogId());
        }
        logs.add(log);
      } catch (IOException e) {
        throw new IOException("Unable to read state log " + oldLogFiles[i], e);
      }
    }
    Collections.sort(logs);

    // If a compacted log is present remove previous logs that were not removed
    if (maxCompactedLogId > 0) {
      removeAllLogs(maxCompactedLogId - 1);
    }

    // Load the most recent tracker available
    Iterator<ProcedureWALFile> it = logs.descendingIterator();
    while (it.hasNext()) {
      ProcedureWALFile log = it.next();
      try {
        log.readTracker(storeTracker);
        break;
      } catch (IOException e) {
        LOG.error("Unable to read tracker for " + log, e);
        // try the next one...
      }
    }

    return logs.getLast().getLogId();
  }

  private void initSyncThread() {
    syncThread = new Thread() {
      @Override
      public void run() {
        while (isRunning.get()) {
          try {
            syncLoop();
          } catch (IOException e) {
            rollWriter(flushLogId + 1);
          }
        }
      }
    };
  }

  private void syncLoop() throws IOException {
    inSync = false;
    while (isRunning.get()) {
      lock.lock();
      try {
        // Wait until new data is available
        if (slotIndex == 0) {
          LOG.debug("Wait for data flushed: " + StringUtils.humanSize(totalSynced));
          waitCond.await();
          if (slotIndex == 0) {
            // no data.. probably a stop()
            continue;
          }
        }

        // Wait SYNC_WAIT_MSEC or the signal of "slots full" before flushing
        slotCond.await(SYNC_WAIT_MSEC, TimeUnit.MILLISECONDS);

        inSync = true;
        totalSynced += syncSlots();
        slotIndex = 0;
        inSync = false;
        syncCond.signalAll();
      } catch (InterruptedException e) {
        // TODO
      } finally {
        lock.unlock();
      }
    }
  }

  private long syncSlots() {
    boolean retry = false;
    long totalSynced = 0;
    do {
      try {
        totalSynced = syncSlots(stream, slots, 0, slotIndex);
      } catch (IOException e) {
        // TODO: Send panic signal!
        retry = true;
      }
    } while (retry);
    return totalSynced;
  }

  protected long syncSlots(FSDataOutputStream stream, ByteSlot[] slots, int offset, int count)
      throws IOException {
    long totalSynced = 0;
    for (int i = 0; i < count; ++i) {
      ByteSlot data = slots[offset + i];
      data.writeTo(stream);
      totalSynced += data.size();
    }
    stream.hsync();
    return totalSynced;
  }

  private boolean rollWriter(final long logId) {
    ProcedureWALHeader header = ProcedureWALHeader.newBuilder()
      .setVersion(ProcedureWALFormat.HEADER_VERSION)
      .setType(ProcedureWALFormat.LOG_TYPE_STREAM)
      .setMinProcId(storeTracker.getMinProcId())
      .setLogId(logId)
      .build();

    FSDataOutputStream newStream = null;
    Path newLogFile = null;
    long startPos = -1;
    try {
      newLogFile = createNewFile(logId);
      newStream = fs.create(newLogFile);
      ProcedureWALFormat.writeHeader(newStream, header);
      startPos = newStream.getPos();
    } catch (IOException e) {
      LOG.error("Unable to create the new log writer: " + e.getMessage());
      return false;
    }
    lock.lock();
    try {
      closeStream();
      synchronized (storeTracker) {
        storeTracker.resetUpdates();
      }
      stream = newStream;
      flushLogId = logId;
      totalSynced = 0;
      logs.add(new ProcedureWALFile(fs, newLogFile, header, startPos));
    } finally {
      lock.unlock();
    }
    LOG.info("Roll new state log: " + logId);
    return true;
  }

  private void closeStream() {
    try {
      if (stream != null) {
        try {
          ProcedureWALFormat.writeTrailer(stream, storeTracker);
        } catch (IOException e) {
          LOG.error("Unable to write the trailer", e);
        }
        stream.close();
      }
    } catch (IOException e) {
      LOG.error("Unable to close the stream", e);
    } finally {
      stream = null;
    }
  }

  private void removeAllLogs(long lastLogId) {
    LOG.info("Remove all state logs with ID less then " + lastLogId);
    while (!logs.isEmpty()) {
      ProcedureWALFile log = logs.getFirst();
      if (lastLogId < log.getLogId()) {
        break;
      }

      removeLogFile(log);
    }
  }

  private boolean removeLogFile(final ProcedureWALFile log) {
    try {
      LOG.debug("remove log: " + log);
      log.removeFile();
      logs.remove(log);
    } catch (IOException e) {
      LOG.error("unable to remove log " + log, e);
      return false;
    }
    return true;
  }

  public Set<ProcedureWALFile> getCorruptedLogs() {
    return corruptedLogs;
  }

  protected abstract Path createNewFile(long logId)
    throws IOException;

  protected abstract FileStatus[] getLogFiles()
    throws IOException;

  protected abstract void serializeProcedure(OutputStream stream, Procedure proc)
    throws IOException;

  protected abstract Procedure deserializeProcedure(long procId, byte[] data)
    throws IOException;
}