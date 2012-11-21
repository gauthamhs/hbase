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
package org.apache.hadoop.hbase.errorhandling;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.ForeignExceptionMessage;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that we correctly serialize exceptions from a remote source
 */
@Category(SmallTests.class)
public class TestForiegnExceptionSerialization {
  private static final String srcName = "someNode";

  /**
   * Verify that we get back similar stack trace information before an after serialization.
   */
  @Test
  public void testSimpleException() {
    String data = "some bytes";
    ForeignException in = new ForeignException(data);
    // check that we get the data back out
    ForeignExceptionMessage msg = ForeignException.toForeignExceptionMessage(srcName, in);
    ForeignException e = ForeignException.unwind(msg);
    assertNotNull(e);

    // now check that we get the right stack trace
    StackTraceElement elem = new StackTraceElement(this.getClass().toString(), "method", "file", 1);
    in.setStackTrace(new StackTraceElement[] { elem });
    msg = ForeignException.toForeignExceptionMessage(srcName, in);
    e = ForeignException.unwind(msg);
    assertNotNull(e);
    assertEquals("Stack trace got corrupted", elem, e.getStackTrace()[0]);
    assertEquals("Got an unexpectedly long stack trace", 1, e.getStackTrace().length);
  }

  /**
   * Compare that a generic exception's stack trace has the same stack trace elements after
   * serialization and deserialization
   */
  @Test
  public void testRemoteFromLocal() {
    String errorMsg = "some message";
    Exception generic = new Exception(errorMsg);
    generic.printStackTrace();
    assertTrue(generic.getMessage().contains(errorMsg));

    ForeignExceptionMessage msg =
        ForeignException.toForeignExceptionMessage(srcName, generic);
    ForeignException e = ForeignException.unwind(msg);
    assertArrayEquals("Local stack trace got corrupted", generic.getStackTrace(), e.getStackTrace());

    e.printStackTrace(); // should have ForeignException and source node in it.
    assertTrue(e.getCause() == null);

    // verify that original error message is present in Foreign exception message
    assertTrue(e.getMessage().contains(errorMsg));
  }

}
