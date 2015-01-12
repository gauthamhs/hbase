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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A SequentialProcedure describes one step in a procedure chain.
 *   -> Step 1 -> Step 2 -> Step 3
 *
 * The main difference from a base Procedure is that the execute() of a
 * SequentialProcedure will be called only once, there will be no second
 * execute() call once the child are finished. which means once the child
 * of a SequentialProcedure are completed the SequentialProcedure is completed too.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class SequentialProcedure<TEnvironment> extends Procedure<TEnvironment> {
  private boolean step = false;

  @Override
  protected Procedure[] doExecute(final TEnvironment env) {
    updateTimestamp();
    return ((step = !step)) ? execute(env) : null;
  }

  @Override
  protected void doRollback(final TEnvironment env) {
    if (!(step = !step)) rollback(env);
  }

  @Override
  protected void serializeStateData(final OutputStream stream) throws IOException {
    stream.write(step ? 1 : 0);
  }

  @Override
  protected void deserializeStateData(final InputStream stream) throws IOException {
    step = (stream.read() == 1);
  }
}