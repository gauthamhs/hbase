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
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.util.CodingUtil;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProcedureStoreTracker {
  private final TreeMap<Long, BitSetNode> map = new TreeMap<Long, BitSetNode>();

  public static class BitSetNode {
    private final static long WORD_MASK = 0xffffffffffffffffL;
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private final static int MAX_NODE_SIZE = 4 << ADDRESS_BITS_PER_WORD;

    private long[] updated;
    private long[] deleted;
    private long start;

    public void dump() {
      System.out.printf("%06d:%06d min=%d max=%d\n", start, getEnd(), getMinProcId(), getMaxProcId());
      System.out.println("Update:");
      for (int i = 0; i < updated.length; ++i) {
        for (int j = 0; j < BITS_PER_WORD; ++j) {
          System.out.print((updated[i] & (1l << j)) != 0 ? "1" : "0");
        }
        System.out.println(" " + i);
      }
      System.out.println();
      System.out.println("Delete:");
      for (int i = 0; i < deleted.length; ++i) {
        for (int j = 0; j < BITS_PER_WORD; ++j) {
          System.out.print((deleted[i] & (1l << j)) != 0 ? "1" : "0");
        }
        System.out.println(" " + i);
      }
      System.out.println();
    }

    public BitSetNode(final long procId) {
      start = alignDown(procId);

      int count = 2;
      updated = new long[count];
      deleted = new long[count];
      for (int i = 0; i < count; ++i) {
        updated[i] = 0;
        deleted[i] = WORD_MASK;
      }

      updateState(procId, false);
    }

    protected BitSetNode(final long start, final long[] updated, final long[] deleted) {
      this.start = start;
      this.updated = updated;
      this.deleted = deleted;
    }

    public void update(final long procId) {
      updateState(procId, false);
    }

    public void delete(final long procId) {
      updateState(procId, true);
    }

    public Long getStart() {
      return start;
    }

    public Long getEnd() {
      return start + (updated.length << ADDRESS_BITS_PER_WORD) - 1;
    }

    public boolean contains(final long procId) {
      return start <= procId && procId <= getEnd();
    }

    public boolean isDeleted(final long procId) {
      int bitmapIndex = getBitmapIndex(procId);
      int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
      return (deleted[wordIndex] & (1 << bitmapIndex)) != 0;
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

    public void writeTo(final OutputStream stream) throws IOException {
      CodingUtil.writeVInt(stream, start);
      CodingUtil.writeVInt(stream, updated.length);
      CodingUtil.packLongs(stream, updated, updated.length);
      CodingUtil.packLongs(stream, deleted, deleted.length);
    }

    public static BitSetNode fromStream(final InputStream stream) throws IOException {
      long start = CodingUtil.readVLong(stream);
      int size = CodingUtil.readVInt(stream);

      long[] updated = new long[size];
      long[] deleted = new long[size];
      CodingUtil.unpackLongs(stream, updated, size);
      CodingUtil.unpackLongs(stream, deleted, size);

      return new BitSetNode(start, updated, deleted);
    }

    // ========================================================================
    //  Grow/Merge Helpers
    // ========================================================================
    public boolean canGrow(final long procId) {
      return (procId - start) < MAX_NODE_SIZE;
    }

    public boolean canMerge(final BitSetNode rightNode) {
      return (start + rightNode.getEnd()) < MAX_NODE_SIZE;
    }

    public void grow(final long procId) {
      int delta, offset;

      if (procId < start) {
        // add to head
        long newStart = alignDown(procId);
        delta = (int)(start - newStart) >> ADDRESS_BITS_PER_WORD;
        offset = delta;
      } else {
        // Add to tail
        long newEnd = alignUp(procId + 1);
        delta = (int)(newEnd - getEnd()) >> ADDRESS_BITS_PER_WORD;
        offset = 0;
      }

      long[] newBitmap;
      int oldSize = updated.length;

      newBitmap = new long[oldSize + delta];
      System.arraycopy(updated, 0, newBitmap, offset, oldSize);
      updated = newBitmap;

      newBitmap = new long[deleted.length + delta];
      System.arraycopy(deleted, 0, newBitmap, offset, oldSize);
      deleted = newBitmap;

      for (int i = 0; i < delta; ++i) {
        updated[oldSize + i] = 0;
        deleted[oldSize + i] = WORD_MASK;
      }
    }

    public void merge(final BitSetNode rightNode) {
      int delta = (int)(rightNode.getEnd() - getEnd()) >> ADDRESS_BITS_PER_WORD;

      long[] newBitmap;
      int oldSize = updated.length;
      int newSize = (delta - rightNode.updated.length);
      int offset = oldSize + newSize;

      newBitmap = new long[oldSize + delta];
      System.arraycopy(updated, 0, newBitmap, 0, oldSize);
      System.arraycopy(rightNode.updated, 0, newBitmap, offset, rightNode.updated.length);
      updated = newBitmap;

      newBitmap = new long[oldSize + delta];
      System.arraycopy(deleted, 0, newBitmap, 0, oldSize);
      System.arraycopy(rightNode.deleted, 0, newBitmap, offset, rightNode.deleted.length);
      deleted = newBitmap;

      for (int i = 0; i < newSize; ++i) {
        updated[offset + i] = 0;
        deleted[offset + i] = WORD_MASK;
      }
    }

    // ========================================================================
    //  Min/Max Helpers
    // ========================================================================
    public long getMinProcId() {
      long minProcId = start;
      for (int i = 0; i < deleted.length; ++i) {
        if (deleted[i] == 0) {
          return(minProcId);
        }

        if (deleted[i] != WORD_MASK) {
          for (int j = 0; j < BITS_PER_WORD; ++j) {
            if ((deleted[i] & (1l << j)) != 0) {
              return minProcId + j;
            }
          }
        }

        minProcId += BITS_PER_WORD;
      }
      return minProcId;
    }

    public long getMaxProcId() {
      long maxProcId = getEnd();
      for (int i = deleted.length - 1; i >= 0; --i) {
        if (deleted[i] == 0) {
          return maxProcId;
        }

        if (deleted[i] != WORD_MASK) {
          for (int j = BITS_PER_WORD - 1; j >= 0; --j) {
            if ((deleted[i] & (1l << j)) == 0) {
              return maxProcId - (BITS_PER_WORD - 1 - j);
            }
          }
        }
        maxProcId -= BITS_PER_WORD;
      }
      return maxProcId;
    }

    // ========================================================================
    //  Bitmap Helpers
    // ========================================================================
    private int getBitmapIndex(final long procId) {
      return (int)(procId - start);
    }

    private void updateState(final long procId, final boolean isDeleted) {
      int bitmapIndex = getBitmapIndex(procId);
      int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
      long value = (1l << bitmapIndex);

      if (isDeleted) {
        updated[wordIndex] |= value;
        deleted[wordIndex] |= value;
      } else {
        updated[wordIndex] |= value;
        deleted[wordIndex] &= ~value;
      }
    }

    protected void setDeleted(final long procId, final boolean isDeleted) {
      int bitmapIndex = getBitmapIndex(procId);
      int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
      long value = (1l << bitmapIndex);

      if (isDeleted) {
        deleted[wordIndex] |= value;
      } else {
        deleted[wordIndex] &= ~value;
      }
    }

    // ========================================================================
    //  Helpers
    // ========================================================================
    private static long alignUp(final long x) {
      return (x + (BITS_PER_WORD - 1)) & -BITS_PER_WORD;
    }

    private static long alignDown(final long x) {
      return x & -BITS_PER_WORD;
    }
  }

  public void insert(final Procedure proc, final Procedure[] subprocs) {
    insert(proc.getProcId());
    if (subprocs != null) {
      for (int i = 0; i < subprocs.length; ++i) {
        insert(subprocs[i].getProcId());
      }
    }
  }

  public void update(final Procedure proc) {
    update(proc.getProcId());
  }

  public void insert(long procId) {
    BitSetNode node = getOrCreateNode(procId);
    node.update(procId);
  }

  public void update(long procId) {
    Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    assert entry != null : "expected node to update procId=" + procId;

    BitSetNode node = entry.getValue();
    assert node.contains(procId);
    node.update(procId);
  }

  public void delete(long procId) {
    Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    assert entry != null : "expected node to delete procId=" + procId;

    BitSetNode node = entry.getValue();
    assert node.contains(procId) : "expected procId in the node";
    node.delete(procId);

    if (node.isEmpty()) {
      if (map.size() > 1) {
        map.remove(entry.getKey());
      } else {
        // TODO: RESET
        map.remove(entry.getKey());
      }
    }
  }

  @InterfaceAudience.Private
  public void setDeleted(final long procId, final boolean isDeleted) {
    BitSetNode node;
    if (isDeleted) {
      Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
      if (entry == null || !entry.getValue().contains(procId))
        return;

      node = entry.getValue();
    } else {
      node = getOrCreateNode(procId);
    }
    node.setDeleted(procId, isDeleted);
  }

  public boolean isDeleted(long procId) {
    Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    return entry == null ? true : entry.getValue().isDeleted(procId);
  }

  public long getMinProcId() {
    Map.Entry<Long, BitSetNode> entry = map.firstEntry();
    return entry == null ? 0 : entry.getValue().getMinProcId();
  }

  public long getMaxProcId() {
    Map.Entry<Long, BitSetNode> entry = map.lastEntry();
    return entry == null ? -1 : entry.getValue().getMaxProcId();
  }

  public boolean isEmpty() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      if (entry.getValue().isEmpty() == false) {
        return false;
      }
    }
    return true;
  }

  public boolean isUpdated() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      if (entry.getValue().isUpdated() == false) {
        return false;
      }
    }
    return true;
  }

  public void resetUpdates() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().resetUpdates();
    }
  }

  public void undeleteAll() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().undeleteAll();
    }
  }

  private BitSetNode getOrCreateNode(long procId) {
    // can procId fit in the left node?
    BitSetNode leftNode = null;
    boolean leftCanGrow = false;
    Map.Entry<Long, BitSetNode> leftEntry = map.floorEntry(procId);
    if (leftEntry != null) {
      leftNode = leftEntry.getValue();
      if (leftNode.contains(procId)) {
        return leftNode;
      }
      leftCanGrow = leftNode.canGrow(procId);
    }

    BitSetNode rightNode = null;
    boolean rightCanGrow = false;
    Map.Entry<Long, BitSetNode> rightEntry = map.ceilingEntry(procId);
    if (rightNode != null) {
      rightCanGrow = rightNode.canGrow(procId);
      if (leftNode != null) {
        if (leftNode.canMerge(rightNode)) {
          // merge left and right node
          return mergeNodes(leftNode, rightNode);
        }

        if (leftCanGrow && rightCanGrow) {
          if ((procId - leftNode.getEnd()) <= (rightNode.getStart() - procId)) {
            // grow the left node
            return growNode(leftNode, procId);
          }
          // grow the right node
          return growNode(rightNode, procId);
        }
      }
    }

    // grow the left node
    if (leftCanGrow) {
      return growNode(leftNode, procId);
    }

    // grow the right node
    if (rightCanGrow) {
      return growNode(rightNode, procId);
    }

    // add new node
    BitSetNode node = new BitSetNode(procId);
    map.put(node.getStart(), node);
    return node;
  }

  private BitSetNode growNode(BitSetNode node, long procId) {
    map.remove(node.getStart());
    node.grow(procId);
    map.put(node.getStart(), node);
    return node;
  }

  private BitSetNode mergeNodes(BitSetNode leftNode, BitSetNode rightNode) {
    leftNode.merge(rightNode);
    map.remove(rightNode.getStart());
    return leftNode;
  }

  public void dump() {
    System.out.println("map " + map.size());
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().dump();
    }
  }

  public void writeTo(final OutputStream stream) throws IOException {
    stream.write(1);
    CodingUtil.writeVInt(stream, map.size());
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().writeTo(stream);
    }
  }

  public void readFrom(final InputStream stream) throws IOException {
    int version = stream.read();
    int n = CodingUtil.readVInt(stream);

    map.clear();
    while (n --> 0) {
      BitSetNode node = BitSetNode.fromStream(stream);
      map.put(node.getStart(), node);
    }
  }
}