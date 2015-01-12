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

package org.apache.hadoop.hbase.procedure2.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ByteSlot extends OutputStream {
  private final int DOUBLE_GROW_LIMIT = 1 << 20;
  private final int GROW_ALIGN = 128;

  private byte[] buf;
  private int head;
  private int size;

  public void reset() {
    head = 0;
    size = 0;
  }

  public void markHead() {
    head = size;
  }

  public int getHead() {
    return head;
  }

  public int size() {
    return size;
  }

  public byte[] getBuffer() {
    return buf;
  }

  public void writeAt(int offset, int b) {
    head = Math.min(head, offset);
    buf[offset] = (byte)b;
  }

  public void write(int b) {
    ensureCapacity(size + 1);
    buf[size++] = (byte)b;
  }

  public void write(byte b[], int off, int len) {
    ensureCapacity(size + len);
    System.arraycopy(b, off, buf, size, len);
    size += len;
  }

  public void writeTo(final OutputStream stream) throws IOException {
    if (head != 0) {
      stream.write(buf, head, size - head);
      stream.write(buf, 0, head);
    } else {
      stream.write(buf, 0, size);
    }
  }

  private void ensureCapacity(int minCapacity) {
    minCapacity = (minCapacity + (GROW_ALIGN - 1)) & -GROW_ALIGN;
    if (buf == null) {
      buf = new byte[minCapacity];
    } else if (minCapacity > buf.length) {
      int newCapacity = buf.length << 1;
      if (minCapacity > newCapacity || newCapacity > DOUBLE_GROW_LIMIT) {
        newCapacity = minCapacity;
      }
      buf = Arrays.copyOf(buf, newCapacity);
    }
  }
}