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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.engine.Procedure;
import org.apache.hadoop.hbase.procedure2.util.CodingUtil;

/**
 * Procedure described by a series of steps
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class StateMachineProcedure<TState> extends Procedure {
  private int stateCount = 0;
  private int[] states = null;

  /**
   * called to perform a single step of the specified 'state' of the procedure
   * @param state state to execute
   * @return true if the procedure is completed, false if there is another step
   */
  protected abstract boolean executeFromState(TState state);

  /**
   * called to perform the rollback of the specified state
   * @param state state to rollback
   */
  protected abstract void rollbackState(TState state);

  /**
   * Convert an ordinal (or state id) to an Enum (or more descriptive) state object.
   * @param stateId the ordinal() of the state enum (or state id)
   * @return the state enum object
   */
  protected abstract TState getState(int stateId);

  @Override
  public Procedure[] execute() {
    updateTimestamp();
    if (executeFromState(stateCount > 0 ? getState(states[stateCount-1]) : null)) {
      // completed
      return null;
    }
    return (isWaiting() || isFailed()) ? null : new Procedure[] {this};
  }

  @Override
  public void rollback() {
    rollbackState(getState(states[--stateCount]));
  }

  /**
   * Set the next state for the procedure
   * @param stateId the ordinal() of the state enum (or state id)
   */
  public void setNextState(final int stateId) {
    if (states == null || states.length == stateCount) {
      int newCapacity = stateCount + 8;
      if (states != null) {
        states = Arrays.copyOf(states, newCapacity);
      } else {
        states = new int[newCapacity];
      }
    }
    states[stateCount++] = stateId;
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    CodingUtil.writeVInt(stream, stateCount);
    CodingUtil.packInts(stream, states, stateCount);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    stateCount = CodingUtil.readVInt(stream);
    states = new int[(stateCount + 7) & -8];
    CodingUtil.unpackInts(stream, states, stateCount);
  }
}