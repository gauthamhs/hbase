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

package org.apache.hadoop.hbase.procedure2.engine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.store.wal.ProtobufProcedureStoreWAL;
import org.apache.hadoop.hbase.procedure2.ProcedureAbortedException;
import org.apache.hadoop.hbase.procedure2.ProcedureResult;
import org.apache.hadoop.hbase.procedure2.SequentialProcedure;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;

import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureExecution {
  private static final Log LOG = LogFactory.getLog(TestProcedureExecution.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final Procedure NULL_PROC = null;

  private ProtobufProcedureStoreWAL procStore;
  private ProcedureExecutor procExecutor;

  private HBaseTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseTestingUtility();
    fs = htu.getTestFileSystem();
    testDir = htu.getDataTestDir();
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procStore = new ProtobufProcedureStoreWAL(fs, logDir);
    procExecutor = new ProcedureExecutor(procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop();
    fs.delete(logDir, true);
  }

  private static class TestProcedureExeption extends Exception {
    public TestProcedureExeption(String msg) { super(msg); }
  }

  private static class TestSequentialProcedure extends SequentialProcedure {
    private final Procedure[] subProcs;
    private final List<String> state;
    private final Exception failure;
    private final String name;

    public TestSequentialProcedure(String name, List<String> state, Procedure... subProcs) {
      this.state = state;
      this.subProcs = subProcs;
      this.name = name;
      this.failure = null;
    }

    public TestSequentialProcedure(String name, List<String> state, Exception failure) {
      this.state = state;
      this.subProcs = null;
      this.name = name;
      this.failure = failure;
    }

    @Override
    public Procedure[] execute() {
      state.add(name + "-execute");
      if (failure != null) {
        setFailure(new ForeignException(name + "-failure", failure));
        return null;
      }
      return subProcs;
    }

    @Override
    public void rollback() {
      state.add(name + "-rollback");
    }

    @Override
    public void abort() {
      state.add(name + "-abort");
    }
  }

  @Test(timeout=60000)
  public void testBadSubprocList() {
    List<String> state = new ArrayList<String>();
    Procedure subProc2 = new TestSequentialProcedure("subProc2", state);
    Procedure subProc1 = new TestSequentialProcedure("subProc1", state, subProc2, NULL_PROC);
    Procedure rootProc = new TestSequentialProcedure("rootProc", state, subProc1);
    long rootId = submitAndWait(rootProc);

    // subProc1 has a "null" subprocedure which is catched as InvalidArgument
    // failed state with 2 execute and 2 rollback
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
      result.getException().getCause() instanceof IllegalArgumentException);
    assertEquals(state.toString(), 4, state.size());
  }

  @Test(timeout=60000)
  public void testSingleSequentialProc() {
    List<String> state = new ArrayList<String>();
    Procedure subProc2 = new TestSequentialProcedure("subProc2", state);
    Procedure subProc1 = new TestSequentialProcedure("subProc1", state, subProc2);
    Procedure rootProc = new TestSequentialProcedure("rootProc", state, subProc1);
    long rootId = submitAndWait(rootProc);

    // successful state, with 3 execute
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    assertProcNotFailed(result);
    assertEquals(state.toString(), 3, state.size());
  }

  @Test(timeout=60000)
  public void testSingleSequentialProcRollback() {
    List<String> state = new ArrayList<String>();
    Procedure subProc3 = new TestSequentialProcedure("subProc3", state);
    Procedure subProc2 = new TestSequentialProcedure("subProc2", state,
                                                     new TestProcedureExeption("fail test"));
    Procedure subProc1 = new TestSequentialProcedure("subProc1", state, subProc2);
    Procedure rootProc = new TestSequentialProcedure("rootProc", state, subProc1);
    long rootId = submitAndWait(rootProc);

    // the 3rd proc fail, rollback after 2 successful execution
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
      result.getException().getCause() instanceof TestProcedureExeption);
    assertEquals(state.toString(), 6, state.size());
  }

  private long submitAndWait(final Procedure proc) {
    long procId = procExecutor.submitProcedure(proc);
    while (!procExecutor.isFinished(procId)) {
      Threads.sleepWithoutInterrupt(250);
    }
    dumpLogDirState();
    return procId;
  }

  private static void assertProcNotFailed(final ProcedureResult result) {
    Exception exception = result.getException();
    String msg = exception != null ? exception.toString() : "no exception found";
    assertFalse(msg, result.isFailed());
  }

  private void assertEmptyLogDir() {
    try {
      FileStatus[] status = fs.listStatus(logDir);
      assertTrue("expected empty state-log dir", status == null || status.length == 0);
    } catch (FileNotFoundException e) {
      fail("expected the state-log dir to be present: " + logDir);
    } catch (IOException e) {
      fail("got en exception on state-log dir list: " + e.getMessage());
    }
  }

  private void dumpLogDirState() {
    try {
      FSUtils.logFileSystemState(fs, testDir, LOG);
    } catch (IOException e) {
      LOG.warn("Unable to dump " + testDir, e);
    }
  }
}