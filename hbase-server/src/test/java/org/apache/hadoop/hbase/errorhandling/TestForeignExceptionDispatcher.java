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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test that we propagate errors through an dispatcher exactly once via different failure
 * injection mechanisms.
 */
@Category(SmallTests.class)
public class TestForeignExceptionDispatcher {
  private static final Log LOG = LogFactory.getLog(TestForeignExceptionDispatcher.class);

  /**
   * Exception thrown from the test
   */
  final ForeignException EXTEXN = new ForeignException("FORTEST");
  final ForeignException EXTEXN2 = new ForeignException("FORTEST2");

  /**
   * Tests that a dispatcher only dispatches only the first exception, and does not propagate
   * subsequent exceptions.
   */
  @Test
  public void testErrorPropagation() {
    ForeignExceptionListener listener1 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionListener listener2 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionDispatcher dispatcher = new ForeignExceptionDispatcher();

    // add the listeners
    dispatcher.addErrorListener(listener1);
    dispatcher.addErrorListener(listener2);

    // create an artificial error
    String message = "Some error";
    Object[] info = new Object[] { "info1" };
    dispatcher.receiveError(message, EXTEXN, info);

    // make sure the listeners got the error
    Mockito.verify(listener1, Mockito.times(1)).receiveError(message, EXTEXN, info);
    Mockito.verify(listener2, Mockito.times(1)).receiveError(message, EXTEXN, info);

    // make sure that we get an exception
    try {
      dispatcher.rethrowException();
      fail("Monitor should have thrown an exception after getting error.");
    } catch (Exception ex) {
      assertTrue("Got an unexpected exception:" + ex, ex == EXTEXN);
      LOG.debug("Got the testing exception!");
    }

    // push another error, which should be not be passed to listeners
    message = "another error";
    info[0] = "info2";
    dispatcher.receiveError(message, EXTEXN2, info);
    Mockito.verify(listener1, Mockito.never()).receiveError(message, EXTEXN2, info);
    Mockito.verify(listener2, Mockito.never()).receiveError(message, EXTEXN2, info);
  }

  @Test
  public void testSingleDispatcherWithTimer() {
    ForeignExceptionListener listener1 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionListener listener2 = Mockito.mock(ForeignExceptionListener.class);

    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher();

    // add the listeners
    monitor.addErrorListener(listener1);
    monitor.addErrorListener(listener2);

    Object info = "message";
    TimeoutExceptionInjector timer = new TimeoutExceptionInjector(monitor, 1000, info);
    timer.start();
    timer.trigger();

    assertTrue("Monitor didn't get timeout", monitor.hasException());

    // verify that that we propagated the error
    Mockito.verify(listener1).receiveError(Mockito.anyString(),
      Mockito.any(TimeoutForeignException.class), Mockito.eq(info));
    Mockito.verify(listener2).receiveError(Mockito.anyString(),
      Mockito.any(TimeoutForeignException.class), Mockito.eq(info));
  }

  /**
   * Test that the dispatcher can receive an error via the timer mechanism.
   */
  @Test
  public void testAttemptTimer() {
    ForeignExceptionListener listener1 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionListener listener2 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionDispatcher orchestrator = new ForeignExceptionDispatcher();

    // add the listeners
    orchestrator.addErrorListener(listener1);
    orchestrator.addErrorListener(listener2);

    // push another error, which should be passed to listeners
    Object[] info = { "timer" };

    // now create a timer and check for that error
    TimeoutExceptionInjector timer = new TimeoutExceptionInjector(orchestrator, 1000, info);
    timer.start();
    timer.trigger();
    // make sure that we got the timer error
    Mockito.verify(listener1, Mockito.times(1)).receiveError(Mockito.anyString(),
      Mockito.any(TimeoutForeignException.class),
      Mockito.argThat(new VarArgMatcher<Object>(Object.class, info)));
    Mockito.verify(listener2, Mockito.times(1)).receiveError(Mockito.anyString(),
      Mockito.any(TimeoutForeignException.class),
      Mockito.argThat(new VarArgMatcher<Object>(Object.class, info)));
  }

  /**
   * Matcher that matches var-args elements
   * @param <T> Type of args to match
   */
  private static class VarArgMatcher<T> extends BaseMatcher<T> {

    private T[] expected;
    private Class<T> clazz;
    private String reason;

    /**
     * Setup the matcher to expect args of the given type
     * @param clazz type of args to expect
     * @param expected expected arguments
     */
    public VarArgMatcher(Class<T> clazz, T... expected) {
      this.expected = expected;
      this.clazz = clazz;
    }

    @Override
    public boolean matches(Object arg0) {
      // null check early exit
      if (expected == null && arg0 == null) return true;

      // single arg matching
      if (clazz.isAssignableFrom(arg0.getClass())) {
        if (expected.length == 1) {
          if (arg0.equals(expected[0])) return true;
          reason = "single argument received, but didn't match argument";
        } else {
          reason = "single argument received, but expected array of args, size = "
              + expected.length;
        }
      } else if (arg0.getClass().isArray()) {
        // array matching
        try {
          @SuppressWarnings("unchecked")
          T[] arg = (T[]) arg0;
          if (Arrays.equals(expected, arg)) return true;
          reason = "Array of args didn't match expected";
        } catch (Exception e) {
          reason = "Exception while matching arguments:" + e.getMessage();
        }
      } else reason = "Object wasn't the same as passed class or not an array";

      // nothing worked - fail
      return false;
    }

    @Override
    public void describeTo(Description arg0) {
      arg0.appendText(reason);
    }
  }
}