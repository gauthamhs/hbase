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

package org.apache.hadoop.hbase.procedure2.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.util.CodingUtil;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProcedureStoreTracker {
  private final static int ADDRESS_BITS_PER_WORD = 6;
  private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
  private static final long WORD_MASK = 0xffffffffffffffffL;
  private static final int ALIGN_ITEMS = 64; // (1 << 20)

  private long[] deleted;
  private long[] updated;

  private long bitmapFirstId = 0;
  private long minId = 0x7fffffffffffffffL;
  private long maxId = 0;

  public ProcedureStoreTracker() {
    int count = calcBitmapSize(ALIGN_ITEMS);
    updated = new long[count];
    deleted = new long[count];
    for (int i = 0; i < count; ++i) {
      updated[i] = 0;
      deleted[i] = WORD_MASK;
    }
  }

  public void readFrom(final InputStream stream) throws IOException {
    int version = stream.read();
    bitmapFirstId = CodingUtil.readVLong(stream);
    int wordInUse = CodingUtil.readVInt(stream);
    deleted = new long[wordInUse];
    updated = new long[wordInUse];
    for (int i = 0; i < wordInUse; ++i) {
      updated[i] = 0;
      deleted[i] = CodingUtil.readLong(stream, 8);
    }

    minId = findMinId(0);
    maxId = findMaxId((wordInUse << ADDRESS_BITS_PER_WORD) - 1);
  }

  public void writeTo(final OutputStream stream) throws IOException {
    stream.write(1);
    int wordInUse = getWordsInUse();
    CodingUtil.writeVInt(stream, bitmapFirstId);
    CodingUtil.writeVInt(stream, wordInUse);
    // TODO: Use nextClearBit()
    for (int i = 0; i < wordInUse; ++i) {
      CodingUtil.writeInt(stream, deleted[i], 8);
    }
  }

  public long getMinProcId() {
    return bitmapFirstId;
  }

  public boolean isUpdated() {
    // TODO: cache the value
    for (int i = 0; i < updated.length; ++i) {
      long deleteMask = ~deleted[i];
      if ((updated[i] & deleteMask) != (WORD_MASK & deleteMask)) {
        return false;
      }
    }
    return true;
  }

  public boolean isEmpty() {
    // TODO: cache the value
    for (int i = 0; i < deleted.length; ++i) {
      if (deleted[i] != WORD_MASK) {
        return false;
      }
    }
    return true;
  }

  public void resetUpdates() {
    for (int i = 0; i < updated.length; ++i) {
      updated[i] = 0;
    }
  }

  public void undeleteAll() {
    for (int i = 0; i < updated.length; ++i) {
      deleted[i] = 0;
    }
  }

  public void insert(final Procedure proc, final Procedure[] subprocs) {
    long procMinId = proc.getProcId();
    long procMaxId = procMinId;
    if (subprocs != null) {
      for (Procedure subproc: subprocs) {
        procMinId = Math.min(procMinId, subproc.getProcId());
        procMaxId = Math.max(procMaxId, subproc.getProcId());
      }
    }

    synchronized (this) {
      minId = Math.min(minId, procMinId);
      if (procMaxId > maxId) {
        adjustBitmap(procMaxId);
        maxId = procMaxId;
      }

      // Update the bitmap state
      updateState(proc.getProcId(), false);
      if (subprocs != null) {
        for (int i = 0; i < subprocs.length; ++i) {
          updateState(subprocs[i].getProcId(), false);
        }
      }
    }
  }

  public void dump(final String state) {
    synchronized (this) {
      System.out.printf(state + " min=%d max=%d bitmapFirst=%d\n", minId, maxId, bitmapFirstId);
      System.out.print("Update: ");
      for (int i = 0; i < updated.length; ++i) {
        for (int j = 0; j < BITS_PER_WORD; ++j) {
          System.out.print((updated[i] & (1l << j)) != 0 ? "1" : "0");
        }
        System.out.print(" ");
      }
      System.out.println();
      System.out.print("Delete: ");
      for (int i = 0; i < deleted.length; ++i) {
        for (int j = 0; j < BITS_PER_WORD; ++j) {
          System.out.print((deleted[i] & (1l << j)) != 0 ? "1" : "0");
        }
        System.out.print(" ");
      }
      System.out.println();
      System.out.println();
    }
  }

  public void update(final Procedure proc) {
    synchronized (this) {
      updateState(proc.getProcId(), false);
    }
  }

  public void delete(final long procId) {
    synchronized (this) {
      updateState(procId, true);

      if (minId == procId) {
        minId = findMinId(getBitmapIndex(minId));
      }
      if (maxId == procId) {
        maxId = findMaxId(getBitmapIndex(maxId));
      }
    }
  }

  private long findMinId(int fromIndex) {
    int nextIndex = nextClearBit(deleted, fromIndex);
    return (nextIndex >= 0) ? bitmapFirstId + nextIndex : 0x7fffffffffffffffL;
  }

  private long findMaxId(int toIndex) {
    return bitmapFirstId + previousClearBit(deleted, toIndex);
  }

  private void updateState(final long procId, boolean isDeleted) {
    if (procId < bitmapFirstId) return;

    int bitmapIndex = getBitmapIndex(procId);
    int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
    long value = (1l << bitmapIndex);

    assert wordIndex < updated.length : "procId=" + procId;

    if (isDeleted) {
      updated[wordIndex] |= value;
      deleted[wordIndex] |= value;
    } else {
      updated[wordIndex] |= value;
      deleted[wordIndex] &= ~value;
    }
  }

  private void adjustBitmap(final long newMaxId) {
    int bitmapLength = (updated.length << ADDRESS_BITS_PER_WORD);
    if ((newMaxId - bitmapFirstId) >= BITS_PER_WORD) {
      int newMinBitmapIndex = getBitmapIndex(minId) >> ADDRESS_BITS_PER_WORD;
      int count = (getWordsInUse() - newMinBitmapIndex);

      bitmapFirstId = (minId & -BITS_PER_WORD);
      int newSize = calcBitmapSize(1 + (newMaxId - bitmapFirstId));
      boolean isResizable = (newSize > updated.length) || (count * 2 < updated.length);
      if (count > 0 && (newMinBitmapIndex > 0 || isResizable)) {
        if (isResizable) {
          System.out.println(" - newSize=" + newSize + " count=" + count +
                             " minId=" + minId + " maxId=" + maxId + " -> " +
                             ((newSize > updated.length) ? "GROW" : "SHRINK"));
          long[] newUpdated = new long[newSize];
          System.arraycopy(updated, newMinBitmapIndex, newUpdated, 0, count);
          updated = newUpdated;

          long[] newDeleted = new long[newSize];
          System.arraycopy(deleted, newMinBitmapIndex, newDeleted, 0, count);
          deleted = newDeleted;
        } else {
          assert newMinBitmapIndex > 0;
          System.arraycopy(updated, newMinBitmapIndex, updated, 0, count);
          System.arraycopy(deleted, newMinBitmapIndex, deleted, 0, count);
        }

        while (count < updated.length) {
          updated[count] = 0;
          deleted[count] = WORD_MASK;
          count++;
        }
      }
    }
  }

  private int getWordsInUse() {
    return 1 + (getBitmapIndex(maxId) >> ADDRESS_BITS_PER_WORD);
  }

  private int getBitmapIndex(final long procId) {
    return (int)(procId - bitmapFirstId);
  }

  private static int calcBitmapSize(long maxItems) {
    assert maxItems > 0;
    maxItems = (maxItems + (ALIGN_ITEMS - 1)) & -ALIGN_ITEMS;
    return (int)(maxItems >> ADDRESS_BITS_PER_WORD);
  }

  private int nextClearBit(final long[] words, final int fromIndex) {
    int wordsInUse = getWordsInUse();
    int block = fromIndex >> ADDRESS_BITS_PER_WORD;
    long word = ~words[block] & (WORD_MASK << fromIndex);

    while (true) {
      if (word != 0)
        return (block * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
      if (++block >= wordsInUse)
        return -1;
      word = ~words[block];
    }
  }

  private int previousClearBit(final long[] words, final int fromIndex) {
    int wordsInUse = getWordsInUse();
    int block = fromIndex >> ADDRESS_BITS_PER_WORD;
    if (block >= wordsInUse) return fromIndex;

    long word = ~words[block] & (WORD_MASK >>> -(fromIndex + 1));
    while (true) {
      if (word != 0)
        return (block + 1) * BITS_PER_WORD -1 - Long.numberOfLeadingZeros(word);
      if (block-- == 0)
        return -1;
      word = ~words[block];
    }
  }

  @InterfaceAudience.Private
  public void setDeleted(final long procId, final boolean isDeleted) {
    if (procId < bitmapFirstId) return;

    adjustBitmap(procId);
    int bitmapIndex = getBitmapIndex(procId);
    int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
    long value = (1l << bitmapIndex);

    assert wordIndex < deleted.length : "procId=" + procId;

    if (isDeleted) {
      deleted[wordIndex] |= value;
    } else {
      deleted[wordIndex] &= ~value;
    }

    if (procId < minId) {
      minId = findMinId(0);
    }

    if (procId > maxId) {
      maxId = findMaxId((deleted.length << ADDRESS_BITS_PER_WORD) - 1);
    }
  }

  @InterfaceAudience.Private
  public boolean isDeleted(final long procId) {
    if (procId < bitmapFirstId) {
      return true;
    }

    // TODO
    if (procId <= maxId) {
      int bitmapIndex = getBitmapIndex(procId);
      int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
      if (wordIndex < deleted.length) {
        return (deleted[wordIndex] & (1l << bitmapIndex)) != 0;
      }
    }

    return false;
  }

  private static void setBits(final long[] words, final int fromIndex, final int toIndex) {
    if (fromIndex == toIndex)
      return;

    int firstWordIndex = fromIndex >> ADDRESS_BITS_PER_WORD;
    int lastWordIndex  = (toIndex - 1) >> ADDRESS_BITS_PER_WORD;
    long firstWordMask = WORD_MASK << fromIndex;
    long lastWordMask  = WORD_MASK >>> -toIndex;
    if (firstWordIndex == lastWordIndex) {
      words[firstWordIndex] |= (firstWordMask & lastWordMask);
    } else {
      words[firstWordIndex] |= firstWordMask;
      for (int i = firstWordIndex + 1; i < lastWordIndex; ++i) {
        words[i] = WORD_MASK;
      }
      words[lastWordIndex] |= lastWordMask;
    }
  }
}