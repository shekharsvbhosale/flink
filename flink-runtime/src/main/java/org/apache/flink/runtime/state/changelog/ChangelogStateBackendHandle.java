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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A state handle to restore state of ChangelogStateBackend. */
// todo: separate commit?
@Internal
public interface ChangelogStateBackendHandle<ReaderContext> extends KeyedStateHandle {
    List<KeyedStateHandle> getBasePart();

    List<StateChangelogHandle<ReaderContext>> getDeltaPart();

    class ChangelogStateBackendHandleImpl<ReaderContext> implements ChangelogStateBackendHandle {
        private final List<KeyedStateHandle> basePart;
        private final List<StateChangelogHandle<ReaderContext>> deltaPart;
        private final KeyGroupRange keyGroupRange;

        public ChangelogStateBackendHandleImpl(
                List<KeyedStateHandle> basePart,
                List<StateChangelogHandle<ReaderContext>> deltaPart,
                KeyGroupRange keyGroupRange) {
            this.basePart = unmodifiableList(basePart);
            this.deltaPart = unmodifiableList(deltaPart);
            this.keyGroupRange = keyGroupRange;
            checkArgument(keyGroupRange.getNumberOfKeyGroups() > 0);
            //            checkArgument(getStateSize() > 0); // todo: re-enable?
        }

        @Override
        public void registerSharedStates(SharedStateRegistry stateRegistry) {
            // todo
        }

        @Override
        public void discardState() throws Exception {
            // todo
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return keyGroupRange;
        }

        @Nullable
        @Override
        @SuppressWarnings("unchecked")
        public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
            KeyGroupRange intersection = this.keyGroupRange.getIntersection(keyGroupRange);
            if (intersection.getNumberOfKeyGroups() == 0) {
                return null;
            }
            List<KeyedStateHandle> basePart =
                    this.basePart.stream()
                            .map(handle -> handle.getIntersection(keyGroupRange))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            List<StateChangelogHandle<ReaderContext>> deltaPart =
                    this.deltaPart.stream()
                            .map(
                                    handle ->
                                            (StateChangelogHandle<ReaderContext>)
                                                    handle.getIntersection(keyGroupRange))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            return new ChangelogStateBackendHandleImpl<>(basePart, deltaPart, intersection);
        }

        @Override
        public long getStateSize() {
            return basePart.stream().mapToLong(StateObject::getStateSize).sum()
                    + deltaPart.stream().mapToLong(StateObject::getStateSize).sum();
        }

        @Override
        public List<KeyedStateHandle> getBasePart() {
            return basePart;
        }

        @Override
        public List<StateChangelogHandle<ReaderContext>> getDeltaPart() {
            return deltaPart;
        }

        public ChangelogStateBackendHandleImpl<ReaderContext> withDelta(
                StateChangelogHandle<ReaderContext> delta) {
            if (delta == null || delta.getStateSize() == 0) {
                return this;
            } else {
                ArrayList<StateChangelogHandle<ReaderContext>> copy = new ArrayList<>(deltaPart);
                copy.add(delta);
                return new ChangelogStateBackendHandleImpl<>(basePart, copy, keyGroupRange);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "keyGroupRange=%s, basePartSize=%d, deltaPartSize=%d",
                    keyGroupRange, basePart.size(), deltaPart.size());
        }
    }
}
