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

package org.apache.hadoop.hbase.procedure2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.procedure2.store.wal.FSProcedureStoreWAL;
import org.apache.hadoop.hbase.procedure2.ProcedureResult;
import org.apache.hadoop.hbase.procedure2.SequentialProcedure;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
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

  private ProcedureExecutor<Void> procExecutor;
  private FSProcedureStoreWAL procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procStore = new FSProcedureStoreWAL(fs, logDir);
    procExecutor = new ProcedureExecutor(null, procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop();
    fs.delete(logDir, true);
  }

  private static class TestProcedureException extends Exception {
    public TestProcedureException(String msg) { super(msg); }
  }

  private static class TestSequentialProcedure extends SequentialProcedure<Void> {
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
    protected Procedure[] execute(Void env) {
      state.add(name + "-execute");
      if (failure != null) {
        setFailure(new RemoteProcedureException(name + "-failure", failure));
        return null;
      }
      return subProcs;
    }

    @Override
    protected void rollback(Void env) {
      state.add(name + "-rollback");
    }

    @Override
    protected void abort(Void env) {
      state.add(name + "-abort");
    }
  }

  @Test(timeout=30000)
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
    assertEquals("rootProc-execute", state.get(0));
    assertEquals("subProc1-execute", state.get(1));
    assertEquals("subProc1-rollback", state.get(2));
    assertEquals("rootProc-rollback", state.get(3));
  }

  @Test(timeout=30000)
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

  @Test(timeout=30000)
  public void testSingleSequentialProcRollback() {
    List<String> state = new ArrayList<String>();
    Procedure subProc2 = new TestSequentialProcedure("subProc2", state,
                                                     new TestProcedureException("fail test"));
    Procedure subProc1 = new TestSequentialProcedure("subProc1", state, subProc2);
    Procedure rootProc = new TestSequentialProcedure("rootProc", state, subProc1);
    long rootId = submitAndWait(rootProc);

    // the 3rd proc fail, rollback after 2 successful execution
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
      result.getException().getCause() instanceof TestProcedureException);

    assertEquals(state.toString(), 6, state.size());
    assertEquals("rootProc-execute", state.get(0));
    assertEquals("subProc1-execute", state.get(1));
    assertEquals("subProc2-execute", state.get(2));
    assertEquals("subProc2-rollback", state.get(3));
    assertEquals("subProc1-rollback", state.get(4));
    assertEquals("rootProc-rollback", state.get(5));
  }

  private static class TestWaitingProcedure extends SequentialProcedure<Void> {
    private final List<String> state;
    private final boolean hasChild;
    private final String name;

    public TestWaitingProcedure(String name, List<String> state, boolean hasChild) {
      this.hasChild = hasChild;
      this.state = state;
      this.name = name;
    }

    @Override
    protected Procedure[] execute(Void env) {
      state.add(name + "-execute");
      setState(ProcedureState.WAITING_TIMEOUT);
      return hasChild ? new Procedure[] { new TestWaitChild() } : null;
    }

    @Override
    protected void rollback(Void env) {
      state.add(name + "-rollback");
    }

    @Override
    protected void abort(Void env) {
      state.add(name + "-abort");
    }

    private class TestWaitChild extends SequentialProcedure<Void> {
      @Override
      protected Procedure[] execute(Void env) {
        state.add(name + "-child-execute");
        return null;
      }

      @Override
      protected void rollback(Void env) {
        state.add(name + "-child-rollback");
      }

      @Override
      protected void abort(Void env) {
        state.add(name + "-child-abort");
      }
    }
  }

  @Test(timeout=30000)
  public void testAbortTimeout() {
    List<String> state = new ArrayList<String>();
    Procedure proc = new TestWaitingProcedure("wproc", state, false);
    proc.setTimeout(2500);
    long rootId = submitAndWait(proc);
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
               result.getException().getCause() instanceof TimeoutException);
    assertEquals(state.toString(), 2, state.size());
    assertEquals("wproc-execute", state.get(0));
    assertEquals("wproc-rollback", state.get(1));
  }

  @Test(timeout=30000)
  public void testAbortTimeoutWithChildren() {
    List<String> state = new ArrayList<String>();
    Procedure proc = new TestWaitingProcedure("wproc", state, true);
    proc.setTimeout(2500);
    long rootId = submitAndWait(proc);
    LOG.info(state);
    ProcedureResult result = procExecutor.getResult(rootId);
    LOG.info(result.getException());
    assertTrue(state.toString(), result.isFailed());
    assertTrue(result.getException().toString(),
               result.getException().getCause() instanceof TimeoutException);
    assertEquals(state.toString(), 4, state.size());
    assertEquals("wproc-execute", state.get(0));
    assertEquals("wproc-child-execute", state.get(1));
    assertEquals("wproc-child-rollback", state.get(2));
    assertEquals("wproc-rollback", state.get(3));
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
      FileStatus[] files = fs.listStatus(logDir);
      if (files != null && files.length > 0) {
        for (FileStatus file: files) {
          assertTrue(file.toString(), file.isFile());
          LOG.debug("log file " + file.getPath() + " size=" + file.getLen());
        }
      } else {
        LOG.debug("no files under: " + logDir);
      }
    } catch (IOException e) {
      LOG.warn("Unable to dump " + logDir, e);
    }
  }
}