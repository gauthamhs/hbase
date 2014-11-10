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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class CodingUtil {
  /*
   * | 111 111 -- | - 111 111 - | -- 111 111 |
   */
  public static void unpackLongs(InputStream in, long[] values, int count)
      throws IOException {
    int i = 0;

    while (count >= 8) {
      int head0 = in.read();
      int head1 = in.read();
      int head2 = in.read();

      values[i++] = readLong(in, 1 + ((head0 >> 5) & 7));
      values[i++] = readLong(in, 1 + ((head0 >> 2) & 7));
      values[i++] = readLong(in, 1 + (((head0 & 3) << 2) | ((head1 >> 7) & 1)));
      values[i++] = readLong(in, 1 + ((head1 >> 4) & 7));
      values[i++] = readLong(in, 1 + ((head1 >> 1) & 7));
      values[i++] = readLong(in, 1 + ((head1 & 1) << 2 | ((head2 >> 6) & 3)));
      values[i++] = readLong(in, 1 + ((head2 >> 3) & 7));
      values[i++] = readLong(in, 1 + (head2 & 7));

      count -= 8;
    }

    if (count > 0) {
      int head0 = in.read();
      int head1 = (count > 2) ? in.read() : 0;
      int head2 = (count > 5) ? in.read() : 0;

      values[i++] = readLong(in, 1 + ((head0 >> 5) & 7));
      if (count > 1) values[i++] = readLong(in, 1 + ((head0 >> 2) & 7));
      if (count > 2) values[i++] = readLong(in, 1 + (((head0 & 3) << 2) | ((head1 >> 7) & 1)));
      if (count > 3) values[i++] = readLong(in, 1 + ((head1 >> 4) & 7));
      if (count > 4) values[i++] = readLong(in, 1 + ((head1 >> 1) & 7));
      if (count > 5) values[i++] = readLong(in, 1 + ((head1 & 1) << 2 | ((head2 >> 6) & 3)));
      if (count > 6) values[i++] = readLong(in, 1 + ((head2 >> 3) & 7));
    }
  }

  public static void packLongs(OutputStream out, long[] values, int count)
      throws IOException {
    int i = 0;
    while (count >= 8) {
      int head0 = 0;
      int head1 = 0;
      int head2 = 0;
      int x;

      head0 |= (getIntSize(values[i+0]) - 1) << 5;
      head0 |= (getIntSize(values[i+1]) - 1) << 2;

      x = getIntSize(values[i+2]) - 1;
      head0 |= (x & 6) >> 1;
      head1 |= (x & 1) << 7;

      head1 |= (getIntSize(values[i+3]) - 1) << 4;
      head1 |= (getIntSize(values[i+4]) - 1) << 1;

      x = getIntSize(values[i+5]) - 1;
      head1 |= ((x & 4) >> 2);
      head2 |= ((x & 3) << 6);

      head2 |= (getIntSize(values[i+6]) - 1) << 3;
      head2 |= (getIntSize(values[i+7]) - 1) << 0;

      out.write(head0);
      out.write(head1);
      out.write(head2);
      writeInt(out, values[i++], 1 + ((head0 >> 5) & 7));
      writeInt(out, values[i++], 1 + ((head0 >> 2) & 7));
      writeInt(out, values[i++], 1 + (((head0 & 3) << 2) | ((head1 >> 7) & 1)));
      writeInt(out, values[i++], 1 + ((head1 >> 4) & 7));
      writeInt(out, values[i++], 1 + ((head1 >> 1) & 7));
      writeInt(out, values[i++], 1 + ((head1 & 1) << 2 | ((head2 >> 6) & 3)));
      writeInt(out, values[i++], 1 + ((head2 >> 3) & 7));
      writeInt(out, values[i++], 1 + (head2 & 7));

      count -= 8;
    }

    if (count > 0) {
      int head0 = 0;
      int head1 = 0;
      int head2 = 0;

      head0 |= (getIntSize(values[i+0]) - 1) << 5;
      if (count > 1) head0 |= (getIntSize(values[i+1]) - 1) << 2;

      if (count > 2) {
        // | --- --- 11 | 1 --- --- - | -- --- --- |
        int x = getIntSize(values[i+2]) - 1;
        head0 |= (x & 6) >> 1;
        head1 |= (x & 1) << 7;
      }

      if (count > 3) head1 |= (getIntSize(values[i+3]) - 1) << 4;
      if (count > 4) head1 |= (getIntSize(values[i+4]) - 1) << 1;

      if (count > 5) {
        // | --- --- -- | - --- --- 1 | 11 --- --- |
        int x = getIntSize(values[i+5]) - 1;
        head1 |= ((x & 4) >> 2);
        head2 |= ((x & 3) << 6);
      }

      if (count > 6) head2 |= (getIntSize(values[i+6]) - 1) << 3;
      if (count > 7) head2 |= (getIntSize(values[i+7]) - 1) << 0;


      out.write(head0);
      if (count > 2) out.write(head1);
      if (count > 5) out.write(head2);

      writeInt(out, values[i++], 1 + ((head0 >> 5) & 7));
      if (count > 1) writeInt(out, values[i++], 1 + ((head0 >> 2) & 7));
      if (count > 2) writeInt(out, values[i++], 1 + (((head0 & 3) << 2) | ((head1 >> 7) & 1)));
      if (count > 3) writeInt(out, values[i++], 1 + ((head1 >> 4) & 7));
      if (count > 4) writeInt(out, values[i++], 1 + ((head1 >> 1) & 7));
      if (count > 5) writeInt(out, values[i++], 1 + ((head1 & 1) << 2 | ((head2 >> 6) & 3)));
      if (count > 6) writeInt(out, values[i++], 1 + ((head2 >> 3) & 7));
    }
  }

  public static void packInts(final OutputStream out, final int[] values, int count)
      throws IOException {
    int i = 0;
    while (count >= 4) {
      int head = 0;
      head |= (getIntSize(values[i+0]) - 1) << 0;
      head |= (getIntSize(values[i+1]) - 1) << 2;
      head |= (getIntSize(values[i+2]) - 1) << 4;
      head |= (getIntSize(values[i+3]) - 1) << 6;

      out.write(head);
      writeInt(out, values[i++], 1 + ((head >> 0) & 0x3));
      writeInt(out, values[i++], 1 + ((head >> 2) & 0x3));
      writeInt(out, values[i++], 1 + ((head >> 4) & 0x3));
      writeInt(out, values[i++], 1 + ((head >> 6) & 0x3));

      count -= 4;
    }

    if (count > 0) {
      int head = 0;
      for (int j = 0; j < count; ++j) {
        head |= (getIntSize(values[i+j]) - 1) << (j * 2);
      }

      out.write(head);
      for (int j = 0; j < count; ++j) {
        writeInt(out, values[i++], 1 + ((head >> (j * 2)) & 0x3));
      }
    }
  }

  // | 11 11 11 11 | v0 | v1 | v2 | v3 |
  public static void unpackInts(final InputStream in, final int[] values, int count)
      throws IOException {
    int i = 0;
    while (count >= 4) {
      int head = in.read();
      if (head < 0) throw new IOException("EOF");

      values[i++] = readInt(in, 1 + ((head >> 0) & 0x3));
      values[i++] = readInt(in, 1 + ((head >> 2) & 0x3));
      values[i++] = readInt(in, 1 + ((head >> 4) & 0x3));
      values[i++] = readInt(in, 1 + ((head >> 6) & 0x3));

      count -= 4;
    }

    if (count > 0) {
      int head = in.read();
      if (head < 0) throw new IOException("EOF");

      for (int j = 0; j < count; ++j) {
        values[i++] = readInt(in, 1 + ((head >> (j * 2)) & 0x3));
      }
    }
  }

  public static void writeInt(final OutputStream out, long value, int length)
      throws IOException {
    assert length <= 8 : "value=" + value + " length=" + length;
    for (int shift = 0; length-- > 0; shift += 8) {
      out.write((byte)((value >> shift) & 0xff));
    }
  }

  public static int readInt(final InputStream in, int length) throws IOException {
    assert length <= 4 : "length=" + length;
    int result = 0;
    for (int shift = 0; length-- > 0; shift += 8) {
      int x = in.read();
      if (x < 0) throw new IOException("EOF");
      result |= (x << shift);
    }
    return result;
  }

  public static long readLong(final InputStream in, int length) throws IOException {
    assert length <= 8 : "length=" + length;
    long result = 0;
    for (int shift = 0; length-- > 0; shift += 8) {
      long x = in.read();
      if (x < 0) throw new IOException("EOF");
      result |= (x << shift);
    }
    return result;
  }

  public static void writeVInt(final OutputStream out, long value) throws IOException {
    while (value >= 128) {
      out.write((byte)((value & 0x7f) | 128));
      value >>= 7;
    }
    out.write((byte)(value & 0x7f));
  }

  public static int readVInt(final InputStream in) throws IOException {
    int result = 0;
    for (int shift = 0; shift <= 28; shift += 7) {
      long b = in.read();
      if (b < 0) throw new IOException("EOF");

      if ((b & 128) != 0) {
        result |= (b & 0x7f) << shift;
      } else {
        result |= (b << shift);
        return result;
      }
    }
    throw new IOException("Invalid VInt");
  }

  public static long readVLong(final InputStream in) throws IOException {
    long result = 0;
    for (int shift = 0; shift <= 63; shift += 7) {
      long b = in.read();
      if (b < 0) throw new IOException("EOF");

      if ((b & 128) != 0) {
        result |= (b & 0x7f) << shift;
      } else {
        result |= (b << shift);
        return result;
      }
    }
    throw new IOException("Invalid VLong");
  }

  public static int getIntSize(long value) {
    if (value < (1L << 32)) {
      if (value < (1L <<  8)) return(1);
      if (value < (1L << 16)) return(2);
      if (value < (1L << 24)) return(3);
      return(4);
    } else {
      if (value < (1L << 40)) return(5);
      if (value < (1L << 48)) return(6);
      if (value < (1L << 56)) return(7);
      return(8);
    }
  }
}