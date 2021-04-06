/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandle;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

class ChangelogStateBackendRestore {
    @FunctionalInterface
    interface BaseBackendBuilder<K>
            extends FunctionWithException<
                    Collection<KeyedStateHandle>, AbstractKeyedStateBackend<K>, Exception> {}

    @FunctionalInterface
    interface DeltaBackendBuilder<K, R>
            extends BiFunctionWithException<
                    AbstractKeyedStateBackend<K>,
                    Collection<ChangelogStateBackendHandle<R>>,
                    ChangelogKeyedStateBackend<K, R>,
                    Exception> {}

    public static <K, T> ChangelogKeyedStateBackend<K, T> restore(
            T context,
            ClassLoader classLoader,
            Collection<ChangelogStateBackendHandle<T>> stateHandles,
            BaseBackendBuilder<K> baseBackendBuilder,
            DeltaBackendBuilder<K, T> changelogBackendBuilder)
            throws Exception {
        Collection<KeyedStateHandle> baseState = extractBaseState(stateHandles);
        AbstractKeyedStateBackend<K> baseBackend = baseBackendBuilder.apply(baseState);
        ChangelogKeyedStateBackend<K, T> changelogBackend =
                changelogBackendBuilder.apply(baseBackend, stateHandles);
        StateChangeLogReader reader = new StateChangeLogReaderImpl();

        for (ChangelogStateBackendHandle<T> handle : stateHandles) {
            if (handle != null) { // null is empty state (no change)
                readHandle(handle, context, classLoader, changelogBackend, reader);
            }
        }
        return changelogBackend;
    }

    private static <K, T> void readHandle(
            ChangelogStateBackendHandle<T> handle,
            T context,
            ClassLoader classLoader,
            ChangelogKeyedStateBackend<K, T> changelogBackend,
            StateChangeLogReader reader)
            throws Exception {
        for (StateChangelogHandle<T> delta : handle.getDeltaPart()) {
            try (CloseableIterator<StateChange> changes = delta.getChanges(context)) {
                while (changes.hasNext()) {
                    StateChange stateChange = changes.next();
                    DataInputViewStreamWrapper in =
                            new DataInputViewStreamWrapper(
                                    new ByteArrayInputStream(stateChange.getChange()));
                    reader.readAndApply(
                            in, stateChange.getKeyGroup(), changelogBackend, classLoader);
                }
            }
        }
    }

    private static <T> Collection<KeyedStateHandle> extractBaseState(
            Collection<ChangelogStateBackendHandle<T>> stateHandles) {
        // null is empty state (no change)
        Preconditions.checkNotNull(stateHandles);
        return stateHandles.stream()
                .filter(Objects::nonNull)
                .map(ChangelogStateBackendHandle::getBasePart)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private ChangelogStateBackendRestore() {}
}
