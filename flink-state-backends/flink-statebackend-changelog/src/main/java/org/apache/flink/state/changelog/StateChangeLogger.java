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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

interface StateChangeLogger<State, Namespace> {
    static <Namespace, State, StateElement> Iterator<StateElement> loggingIterator(
            @Nullable Iterator<StateElement> iterator,
            StateChangeLogger<State, Namespace> changeLogger,
            BiConsumerWithException<StateElement, DataOutputViewStreamWrapper, IOException>
                    changeWriter,
            Namespace ns) {
        if (iterator == null) {
            return null;
        }
        return new Iterator<StateElement>() {

            @Nullable private StateElement lastReturned;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public StateElement next() {
                return lastReturned = iterator.next();
            }

            @Override
            public void remove() {
                try {
                    changeLogger.stateElementRemoved(
                            out -> changeWriter.accept(lastReturned, out), ns);
                } catch (IOException e) {
                    ExceptionUtils.rethrow(e);
                }
                iterator.remove();
            }
        };
    }

    static <Namespace, State, StateElement> Iterable<StateElement> loggingIterable(
            @Nullable Iterable<StateElement> iterable,
            KvStateChangeLogger<State, Namespace> changeLogger,
            BiConsumerWithException<StateElement, DataOutputViewStreamWrapper, IOException>
                    changeWriter,
            Namespace ns) {
        if (iterable == null) {
            return null;
        }
        return () -> loggingIterator(iterable.iterator(), changeLogger, changeWriter, ns);
    }

    static <UK, UV, State, Namespace> Map.Entry<UK, UV> loggingMapEntry(
            Map.Entry<UK, UV> entry,
            KvStateChangeLogger<State, Namespace> changeLogger,
            BiConsumerWithException<Map.Entry<UK, UV>, DataOutputViewStreamWrapper, IOException>
                    changeWriter,
            Namespace ns) {
        return new Map.Entry<UK, UV>() {
            @Override
            public UK getKey() {
                return entry.getKey();
            }

            @Override
            public UV getValue() {
                return entry.getValue();
            }

            @Override
            public UV setValue(UV value) {
                try {
                    changeLogger.stateElementChanged(out -> changeWriter.accept(entry, out), ns);
                } catch (IOException e) {
                    ExceptionUtils.rethrow(e);
                }
                return entry.setValue(value);
            }
        };
    }

    void stateUpdated(State newState, Namespace ns) throws IOException;

    void stateAdded(State addedState, Namespace ns) throws IOException;

    void stateCleared(Namespace ns);

    void stateElementChanged(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Namespace ns)
            throws IOException;

    void stateElementRemoved(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Namespace ns)
            throws IOException;
}
