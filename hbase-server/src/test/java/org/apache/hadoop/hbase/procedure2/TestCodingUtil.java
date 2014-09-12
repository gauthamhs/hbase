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
import java.io.*;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.procedure2.util.TimeoutBlockingQueue.TimeoutRetriever;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, SmallTests.class})
public class TestCodingUtil {
  @Test
  public void testPackInts() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int[] ovec = new int[128];
    for (int i = 1; i <= ovec.length; ++i) {
      out.reset();
      for (int j = 0; j < i; ++j) {
        ovec[j] = i - j;
      }
      CodingUtil.packInts(out, ovec, i);
      byte[] data = out.toByteArray();

      int[] ivec = new int[i];
      ByteArrayInputStream in = new ByteArrayInputStream(data);
      CodingUtil.unpackInts(in, ivec, ivec.length);
      for (int j = 0; j < ivec.length; ++j) {
        assertEquals(ivec[j], ovec[j]);
      }
    }
  }

  @Test
  public void testPackLongs() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long[] ovec = new long[128];
    for (int i = 1; i <= ovec.length; ++i) {
      out.reset();
      for (int j = 0; j < i; ++j) {
        ovec[j] = i - j;
      }
      CodingUtil.packLongs(out, ovec, i);
      byte[] data = out.toByteArray();

      long[] ivec = new long[i];
      ByteArrayInputStream in = new ByteArrayInputStream(data);
      CodingUtil.unpackLongs(in, ivec, ivec.length);
      for (int j = 0; j < ivec.length; ++j) {
        assertEquals(ivec[j], ovec[j]);
      }
    }
  }
}
