/*
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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.state.changelog.StateChangeLogReader.ChangeApplier;
import org.apache.flink.util.function.BiConsumerWithException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.apache.flink.state.changelog.StateChangeLogger.loggingIterable;
import static org.apache.flink.state.changelog.StateChangeLogger.loggingIterator;
import static org.apache.flink.state.changelog.StateChangeLogger.loggingMapEntry;

/**
 * Delegated partitioned {@link MapState} that forwards changes to {@link StateChange} upon {@link
 * MapState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
class ChangelogMapState<K, N, UK, UV>
        extends AbstractChangelogState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    ChangelogMapState(
            InternalMapState<K, N, UK, UV> delegatedState,
            KvStateChangeLogger<Map<UK, UV>, N> changeLogger) {
        super(delegatedState, changeLogger);
    }

    @Override
    public UV get(UK key) throws Exception {
        return delegatedState.get(key);
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        if (getValueSerializer() instanceof MapSerializer) {
            changeLogger.stateElementChanged(
                    out -> {
                        serializeKey(key, out);
                        serializeValue(value, out);
                    },
                    getCurrentNamespace());
        } else {
            changeLogger.stateAdded(singletonMap(key, value), getCurrentNamespace());
        }
        delegatedState.put(key, value);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        changeLogger.stateAdded(map, getCurrentNamespace());
        delegatedState.putAll(map);
    }

    @Override
    public void remove(UK key) throws Exception {
        changeLogger.stateElementRemoved(out -> serializeKey(key, out), getCurrentNamespace());
        delegatedState.remove(key);
    }

    @Override
    public boolean contains(UK key) throws Exception {
        return delegatedState.contains(key);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        Iterator<Map.Entry<UK, UV>> iterator = delegatedState.entries().iterator();
        final N currentNamespace = getCurrentNamespace();
        return () ->
                loggingIterator(
                        new Iterator<Map.Entry<UK, UV>>() {
                            @Override
                            public Map.Entry<UK, UV> next() {
                                return loggingMapEntry(
                                        iterator.next(),
                                        changeLogger,
                                        changeWriter,
                                        currentNamespace);
                            }

                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public void remove() {
                                iterator.remove();
                            }
                        },
                        changeLogger,
                        changeWriter,
                        currentNamespace);
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        return loggingIterable(
                delegatedState.keys(), changeLogger, this::serializeKey, getCurrentNamespace());
    }

    @Override
    public Iterable<UV> values() throws Exception {
        Iterator<Map.Entry<UK, UV>> iterator = entries().iterator();
        return () ->
                new Iterator<UV>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public UV next() {
                        return iterator.next().getValue();
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return loggingIterator(
                delegatedState.iterator(), changeLogger, changeWriter, getCurrentNamespace());
    }

    @Override
    public boolean isEmpty() throws Exception {
        return delegatedState.isEmpty();
    }

    @Override
    public void clear() {
        changeLogger.stateCleared(getCurrentNamespace());
        delegatedState.clear();
    }

    private void serializeValue(
            UV value, org.apache.flink.core.memory.DataOutputViewStreamWrapper out)
            throws IOException {
        getMapSerializer().getValueSerializer().serialize(value, out);
    }

    private void serializeKey(UK key, org.apache.flink.core.memory.DataOutputViewStreamWrapper out)
            throws IOException {
        getMapSerializer().getKeySerializer().serialize(key, out);
    }

    private MapSerializer<UK, UV> getMapSerializer() {
        return (MapSerializer<UK, UV>) getValueSerializer();
    }

    private final BiConsumerWithException<
                    Map.Entry<UK, UV>, DataOutputViewStreamWrapper, IOException>
            changeWriter = (entry, out) -> serializeKey(entry.getKey(), out);

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> mapState, KvStateChangeLogger<SV, N> changeLogger) {
        return (IS)
                new ChangelogMapState<>(
                        (InternalMapState<K, N, UK, UV>) mapState,
                        (KvStateChangeLogger<Map<UK, UV>, N>) changeLogger);
    }

    @Override
    public ChangeApplier<K, N> getChangeApplier(ChangelogApplierFactory factory) {
        return factory.forMap(this.delegatedState, ctx);
    }
}
