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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.procedure2.store.wal.ProtobufProcedureStoreWAL;
import org.apache.hadoop.hbase.procedure2.ProcedureAbortedException;
import org.apache.hadoop.hbase.procedure2.ProcedureResult;
import org.apache.hadoop.hbase.procedure2.SequentialProcedure;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
public class TestProcedureRecovery {
  private static final Log LOG = LogFactory.getLog(TestProcedureRecovery.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final Procedure NULL_PROC = null;

  private static ProtobufProcedureStoreWAL procStore;
  private static ProcedureExecutor procExecutor;
  private static int procSleepInterval;

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
    procStore = new ProtobufProcedureStoreWAL(fs, logDir);
    procExecutor = new ProcedureExecutor(procStore);
    procExecutor.testing = new ProcedureExecutor.Testing();
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS);
    procSleepInterval = 0;
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop();
    fs.delete(logDir, true);
  }

  private void restart() throws Exception {
    dumpLogDirState();
    procExecutor.stop();
    procStore.stop();
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    ProcedureExecutor.Testing testing = procExecutor.testing;
    procExecutor = new ProcedureExecutor(procStore);
    procExecutor.testing = testing;
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS);
    dumpLogDirState();
  }

  private static void toggleKillBeforeStoreUpdate() {
    procExecutor.testing.killBeforeStoreUpdate = !procExecutor.testing.killBeforeStoreUpdate;
    LOG.debug("Kill before store update " + procExecutor.testing.killBeforeStoreUpdate);
  }

  public static class TestSingleStepProcedure extends SequentialProcedure {
    private int step = 0;

    public TestSingleStepProcedure() { }

    @Override
    public Procedure[] execute() {
      LOG.debug("execute procedure " + this + " step=" + step);
      step++;
      setResult(Bytes.toBytes(step));
      return null;
    }

    @Override
    public void rollback() { }

    @Override
    public void abort() { }
  }

  public static class BaseTestStepProcedure extends SequentialProcedure {
    private AtomicBoolean abort = new AtomicBoolean(false);
    private int step = 0;

    @Override
    public Procedure[] execute() {
      LOG.debug("execute procedure " + this + " step=" + step);
      toggleKillBeforeStoreUpdate();
      step++;
      Threads.sleepWithoutInterrupt(procSleepInterval);
      if (isAborted()) {
        setFailure(new RemoteProcedureException(getClass().getName(), new ProcedureAbortedException(
            "got an abort at " + getClass().getName() + " step=" + step)));
        return null;
      }
      return null;
    }

    @Override
    public void rollback() {
      LOG.debug("rollback procedure " + this + " step=" + step);
      toggleKillBeforeStoreUpdate();
      step++;
    }

    @Override
    public void abort() {
      abort.set(true);
    }

    private boolean isAborted() {
      boolean aborted = abort.get();
      BaseTestStepProcedure proc = this;
      while (proc.hasParent() && !aborted) {
        proc = (BaseTestStepProcedure)procExecutor.getProcedure(proc.getParentProcId());
        aborted = proc.isAborted();
      }
      return aborted;
    }
  }

  public static class TestMultiStepProcedure extends BaseTestStepProcedure {
    public TestMultiStepProcedure() { }

    @Override
    public Procedure[] execute() {
      super.execute();
      return isFailed() ? null : new Procedure[] { new Step1Procedure() };
    }

    public static class Step1Procedure extends BaseTestStepProcedure {
      public Step1Procedure() { }

      @Override
      public Procedure[] execute() {
        super.execute();
        return isFailed() ? null : new Procedure[] { new Step2Procedure() };
      }
    }

    public static class Step2Procedure extends BaseTestStepProcedure {
      public Step2Procedure() { }
    }
  }

  @Test
  public void testNoopLoad() throws Exception {
    restart();
  }

  @Test(timeout=60000)
  public void testSingleStepProcRecovery() throws Exception {
    Procedure proc = new TestSingleStepProcedure();
    procExecutor.testing.killBeforeStoreUpdate = true;
    long procId = submitAndWait(proc);
    assertFalse(procExecutor.isRunning());
    procExecutor.testing.killBeforeStoreUpdate = false;

    // Restart and verify that the procedures restart
    long restartTs = EnvironmentEdgeManager.currentTime();
    restart();
    waitProcedure(procId);
    ProcedureResult result = procExecutor.getResult(procId);
    assertTrue(result.getTimestamp() > restartTs);
    assertProcNotFailed(result);
    assertEquals(1, Bytes.toInt(result.getResult()));
    long resultTs = result.getTimestamp();

    // Verify that after another restart the result is still there
    restart();
    result = procExecutor.getResult(procId);
    assertProcNotFailed(result);
    assertEquals(resultTs, result.getTimestamp());
    assertEquals(1, Bytes.toInt(result.getResult()));
  }

  @Test(timeout=60000)
  public void testMultiStepProcRecovery() throws Exception {
    // Step 0 - kill
    Procedure proc = new TestMultiStepProcedure();
    long procId = submitAndWait(proc);
    assertFalse(procExecutor.isRunning());

    // Step 0 exec && Step 1 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Step 1 exec && step 2 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Step 2 exec
    restart();
    waitProcedure(procId);
    assertTrue(procExecutor.isRunning());

    // The procedure is completed
    ProcedureResult result = procExecutor.getResult(procId);
    assertProcNotFailed(result);
  }

  @Test(timeout=60000)
  public void testMultiStepRollbackRecovery() throws Exception {
    // Step 0 - kill
    Procedure proc = new TestMultiStepProcedure();
    long procId = submitAndWait(proc);
    assertFalse(procExecutor.isRunning());

    // Step 0 exec && Step 1 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Step 1 exec && step 2 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Step 2 exec - rollback - kill
    procSleepInterval = 2500;
    restart();
    assertTrue(procExecutor.abort(procId));
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // rollback - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // rollback - complete
    restart();
    waitProcedure(procId);
    assertTrue(procExecutor.isRunning());

    // The procedure is completed
    ProcedureResult result = procExecutor.getResult(procId);
    assertIsAbortException(result);
  }

  static class TestStateMachineProcedure
      extends StateMachineProcedure<TestStateMachineProcedure.State> {
    enum State { STATE_1, STATE_2, STATE_3, DONE }

    public TestStateMachineProcedure() {}

    private AtomicBoolean aborted = new AtomicBoolean(false);
    private int iResult = 0;

    @Override
    protected boolean executeFromState(State state) {
      if (state == null) {
        LOG.info("Initializing " + this);
        state = State.STATE_1;
        setNextState(state);
      }

      switch (state) {
        case STATE_1:
          LOG.info("execute step 1 " + this);
          toggleKillBeforeStoreUpdate();
          setNextState(State.STATE_2);
          iResult += 3;
          break;
        case STATE_2:
          LOG.info("execute step 2 " + this);
          toggleKillBeforeStoreUpdate();
          setNextState(State.STATE_3);
          iResult += 5;
          break;
        case STATE_3:
          LOG.info("execute step 3 " + this);
          Threads.sleepWithoutInterrupt(procSleepInterval);
          toggleKillBeforeStoreUpdate();
          if (aborted.get()) {
            LOG.info("aborted step 3 " + this);
            setAbortFailure();
            return false;
          }
          setNextState(State.DONE);
          iResult += 7;
          setResult(Bytes.toBytes(iResult));
          return true;
      }
      return false;
    }

    @Override
    protected void rollbackState(final State state) {
      switch (state) {
        case STATE_1:
          LOG.info("rollback step 1 " + this);
          break;
        case STATE_2:
          LOG.info("rollback step 2 " + this);
          toggleKillBeforeStoreUpdate();
          break;
        case STATE_3:
          LOG.info("rollback step 3 " + this);
          toggleKillBeforeStoreUpdate();
          break;
      }
    }

    @Override
    protected State getState(final int stateId) {
      System.out.println("GET STATE " + stateId + " values " + State.values());
      return State.values()[stateId];
    }

    private void setNextState(final State state) {
      setNextState(state.ordinal());
    }

    @Override
    public void abort() {
      aborted.set(true);
    }

    @Override
    public void serializeStateData(final OutputStream stream) throws IOException {
      super.serializeStateData(stream);
      stream.write(Bytes.toBytes(iResult));
    }

    @Override
    public void deserializeStateData(final InputStream stream) throws IOException {
      super.deserializeStateData(stream);
      byte[] data = new byte[4];
      stream.read(data);
      iResult = Bytes.toInt(data);
    }
  }

  @Test(timeout=60000)
  public void testStateMachineRecovery() throws Exception {
    // Step 1 - kill
    Procedure proc = new TestStateMachineProcedure();
    long procId = submitAndWait(proc);
    assertFalse(procExecutor.isRunning());

    // Step 1 exec && Step 2 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Step 2 exec && step 3 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Step 3 exec
    restart();
    waitProcedure(procId);
    assertTrue(procExecutor.isRunning());

    // The procedure is completed
    ProcedureResult result = procExecutor.getResult(procId);
    assertProcNotFailed(result);
    assertEquals(15, Bytes.toInt(result.getResult()));
  }

  @Test(timeout=20000)
  public void testStateMachineRollbackRecovery() throws Exception {
    // Step 1 - kill
    Procedure proc = new TestStateMachineProcedure();
    long procId = submitAndWait(proc);
    assertFalse(procExecutor.isRunning());

    // Step 1 exec && Step 2 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Step 2 exec && step 3 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Step 3 exec - rollback step 3 - kill
    procSleepInterval = 2500;
    restart();
    assertTrue(procExecutor.abort(procId));
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Rollback step 3 - rollback step 2 - kill
    restart();
    waitProcedure(procId);
    assertFalse(procExecutor.isRunning());

    // Rollback step 2 - step 1 - complete
    restart();
    waitProcedure(procId);
    assertTrue(procExecutor.isRunning());

    // The procedure is completed
    ProcedureResult result = procExecutor.getResult(procId);
    assertIsAbortException(result);
  }

  private long submitAndWait(final Procedure proc) {
    long procId = procExecutor.submitProcedure(proc);
    waitProcedure(procId);
    return procId;
  }

  private void waitProcedure(final long procId) {
    while (!procExecutor.isFinished(procId) && procExecutor.isRunning()) {
      Threads.sleepWithoutInterrupt(250);
    }
    dumpLogDirState();
  }

  private static void assertProcNotFailed(final ProcedureResult result) {
    Exception exception = result.getException();
    String msg = exception != null ? exception.toString() : "no exception found";
    assertFalse(msg, result.isFailed());
  }

  private static void assertIsAbortException(final ProcedureResult result) {
    LOG.info(result.getException());
    assertTrue(result.isFailed());
    // TODO: Fix the RemoteProcedureException to store the ExceptionType
    //       and assert on ProcedureAbortedException
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