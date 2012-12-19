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
package org.apache.hadoop.hbase.util;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a map-like object that maps a key to a weakly referenced value.  While it has
 * {@link #get(Object)} and {@link #put(Object, Object)} like a map, this is not a subclass of
 * Map because several methods (e.g. size) wouldn't have the same semantics of a Map or
 * Collections would have.
 *
 * References to value objects are weak so that the values can be GC'ed if all other references
 * are GC'able.  If a referenced value is GC'ed the provided methods will filter out or remove
 * reference entries on gets and full set retrievals.
 *
 * This class is not threadsafe.
 *
 * @param <K> Key
 * @param <V> Value
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WeakValueMapping<K,V> {
  private final Map<K, WeakReference<V>> map = new HashMap<K,WeakReference<V>>();

  /**
   * Get the value associated with the specified key.  It removes empty weak references if it encounters them.
   * Not threadsafe.
   * @param key
   * @return null if the key is not present in the key set.  This could mean that it was never present
   *    or that it has been gc'ed.
   */
  public V get(K key) {
    WeakReference<V> valRef = map.get(key);
    if (valRef == null) {
      return null;
    }

    V val = valRef.get();
    if (val == null) {
      map.remove(key);
    }

    return val;
  }

  /**
   * Put a value associated with the specified key into the mapping. Not threadsafe.
   * @param key
   * @param value
   * @return the previous value the key mapped to if there was one present.
   */
  public V put(K key, V value) {
    V prev = get(key);
    map.put(key, new WeakReference<V>(value));
    return prev;
  }

  /**
   * Remove a value associated with the specified key from the mapping.
   * @param key
   * @return the value associated with the key that we've removed from the mapping
   */
  public V remove(K key) {
    V prev = get(key);
    map.remove(key);
    return prev;
  }

  /**
   * Retrieves the entire set of values.  It checks the weak references and removes empty weak
   * references if it encounters them.  Not threadsafe.
   * @return set of normal references to all the live value objects in the set.
   */
  public Set<V> liveSet() {
    HashSet<V> liveSet = new HashSet<V>();
    List<K> toRemove = new ArrayList<K>();
    for (Entry<K, WeakReference<V>> e : map.entrySet()) {
      WeakReference<V> valRef = e.getValue();
      if (valRef == null) {
        // weak reference was null, ignore.
        continue;
      }

      V val = valRef.get();
      if (val == null) {
        // an empty weak ref, remove
        toRemove.add(e.getKey());
        continue;
      }
      liveSet.add(val);
    }

    // avoid concurrent modification exception
    for (K k : toRemove) {
      map.remove(k);
    }

    return liveSet;
  }

}
