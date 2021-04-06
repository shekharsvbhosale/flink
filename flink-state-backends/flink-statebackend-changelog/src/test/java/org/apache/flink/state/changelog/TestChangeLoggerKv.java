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

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.Collection;

class TestChangeLoggerKv<State> implements KvStateChangeLogger<State, String> {
    boolean stateUpdated;
    boolean stateAdded;
    boolean stateCleared;
    boolean stateElementChanged;
    boolean stateElementRemoved;
    boolean stateMerged;

    @Override
    public void stateUpdated(State newState, String ns) {
        stateUpdated = true;
    }

    @Override
    public void stateAdded(State addedState, String ns) {
        stateAdded = true;
    }

    @Override
    public void stateCleared(String ns) {
        stateCleared = true;
    }

    @Override
    public void stateElementChanged(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, String ns) {
        stateElementChanged = true;
    }

    @Override
    public void stateElementRemoved(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, String ns) {
        stateElementRemoved = true;
    }

    @Override
    public void stateMerged(String target, Collection<String> sources) {
        stateMerged = true;
    }

    public boolean anythingChanged() {
        return stateUpdated
                || stateAdded
                || stateCleared
                || stateElementChanged
                || stateElementRemoved
                || stateMerged;
    }
}
