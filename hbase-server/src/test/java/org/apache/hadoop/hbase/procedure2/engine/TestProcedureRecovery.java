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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.store.wal.ProtobufProcedureStoreWAL;
import org.apache.hadoop.hbase.procedure2.ProcedureAbortedException;
import org.apache.hadoop.hbase.procedure2.ProcedureResult;
import org.apache.hadoop.hbase.procedure2.SequentialProcedure;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
public class TestProcedureRecovery {
  private static final Log LOG = LogFactory.getLog(TestProcedureRecovery.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final Procedure NULL_PROC = null;

  private static ProtobufProcedureStoreWAL procStore;
  private static ProcedureExecutor procExecutor;
  private static int procSleepInterval;

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
      toggleKillBeforeStoreUpdate();
      step++;
      Threads.sleepWithoutInterrupt(procSleepInterval);
      if (isAborted()) {
        setFailure(new ForeignException(getClass().getName(), new ProcedureAbortedException(
            "got an abort at " + getClass().getName() + " step=" + step)));
        return null;
      }
      return null;
    }

    @Override
    public void rollback() {
      // TODO: toggleKillBeforeStoreUpdate();
      LOG.debug("rollback procedure " + this + " step=" + step);
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

  @Test
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

  @Test
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

  @Test
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

    // Step 2 exec - rollback
    procSleepInterval = 2500;
    restart();
    assertTrue(procExecutor.abort(procId));
    waitProcedure(procId);
    assertTrue(procExecutor.isRunning());

    // TODO: make another unit test and restart() every rollback step

    ProcedureResult result = procExecutor.getResult(procId);
    LOG.info(result.getException());
    assertTrue(result.isFailed());
    assertTrue(result.getException().toString(),
      result.getException().getCause() instanceof ProcedureAbortedException);
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

  private void dumpLogDirState() {
    try {
      FSUtils.logFileSystemState(fs, testDir, LOG);
    } catch (IOException e) {
      LOG.warn("Unable to dump " + testDir, e);
    }
  }
}