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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StringUtils {
  public static String humanTimeDiff(long timeDiff) {
    StringBuilder buf = new StringBuilder();
    long hours = timeDiff / (60*60*1000);
    long rem = (timeDiff % (60*60*1000));
    long minutes =  rem / (60*1000);
    rem = rem % (60*1000);
    long seconds = rem / 1000;

    if (hours != 0){
      buf.append(hours);
      buf.append("hrs, ");
    }
    if (minutes != 0){
      buf.append(minutes);
      buf.append("mins, ");
    }
    // return "0sec if no difference
    buf.append(seconds);
    buf.append("sec");
    return buf.toString();
  }

  public static String humanSize(double size) {
    if (size >= (1l << 30)) return String.format("%.1fG", size / (1l << 30));
    if (size >= (1l << 20)) return String.format("%.1fM", size / (1l << 20));
    if (size >= (1l << 10)) return String.format("%.1fK", size / (1l << 10));
    return String.format("%.0f", size);
  }
}