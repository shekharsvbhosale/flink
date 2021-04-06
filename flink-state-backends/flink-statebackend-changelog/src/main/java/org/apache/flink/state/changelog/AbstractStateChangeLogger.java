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
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.ADD;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.CHANGE_ELEMENT;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.CLEAR;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.METADATA;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.REMOVE_ELEMENT;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.SET;
import static org.apache.flink.util.Preconditions.checkNotNull;

abstract class AbstractStateChangeLogger<Key, State, Ns> implements StateChangeLogger<State, Ns> {
    private static final int COMMON_KEY_GROUP = -1;
    protected final StateChangelogWriter<?> stateChangelogWriter;
    protected final InternalKeyContext<Key> keyContext;
    protected final RegisteredStateMetaInfoBase metaInfo;
    private boolean metaDataWritten = false;

    public AbstractStateChangeLogger(
            StateChangelogWriter<?> stateChangelogWriter,
            InternalKeyContext<Key> keyContext,
            RegisteredStateMetaInfoBase metaInfo) {
        this.stateChangelogWriter = checkNotNull(stateChangelogWriter);
        this.keyContext = checkNotNull(keyContext);
        this.metaInfo = checkNotNull(metaInfo);
    }

    @Override
    public void stateUpdated(State newState, Ns ns) throws IOException {
        if (newState == null) {
            stateCleared(ns);
        } else {
            log(SET, out -> serializeState(newState, out), ns);
        }
    }

    protected abstract void serializeState(State state, DataOutputViewStreamWrapper out)
            throws IOException;

    @Override
    public void stateAdded(State addedState, Ns ns) throws IOException {
        log(ADD, out -> serializeState(addedState, out), ns);
    }

    @Override
    public void stateCleared(Ns ns) {
        try {
            log(CLEAR, out -> {}, ns);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void stateElementChanged(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Ns ns)
            throws IOException {
        log(CHANGE_ELEMENT, dataSerializer, ns);
    }

    @Override
    public void stateElementRemoved(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Ns ns)
            throws IOException {
        log(REMOVE_ELEMENT, dataSerializer, ns);
    }

    protected void log(
            StateChangeOperation op,
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataWriter,
            Ns ns)
            throws IOException {
        logMetaIfNeeded();
        stateChangelogWriter.append(
                keyContext.getCurrentKeyGroupIndex(), serialize(op, ns, dataWriter));
    }

    private void logMetaIfNeeded() throws IOException {
        if (metaDataWritten) {
            return;
        }
        stateChangelogWriter.append(
                COMMON_KEY_GROUP,
                serializeRaw(
                        out -> {
                            out.writeByte(METADATA.code);
                            out.writeInt(CURRENT_STATE_META_INFO_SNAPSHOT_VERSION);
                            StateMetaInfoSnapshotReadersWriters.getWriter()
                                    .writeStateMetaInfoSnapshot(metaInfo.snapshot(), out);
                        }));
        metaDataWritten = true;
    }

    private byte[] serialize(
            StateChangeOperation op,
            Ns ns,
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataWriter)
            throws IOException {
        return serializeRaw(
                wrapper -> {
                    wrapper.writeByte(op.code);
                    wrapper.writeUTF(metaInfo.getName()); // todo: wrapper.writeShort(stateId);
                    serializeScope(ns, wrapper);
                    dataWriter.accept(wrapper);
                });
    }

    protected abstract void serializeScope(Ns ns, DataOutputViewStreamWrapper out)
            throws IOException;

    private byte[] serializeRaw(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataWriter)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(out)) {
            dataWriter.accept(wrapper);
            return out.toByteArray();
        }
    }

    enum StateChangeOperation {
        /** Scope: key + namespace. */
        CLEAR((byte) 0),
        /** Scope: key + namespace. */
        SET((byte) 1),
        /** Scope: key + namespace. */
        ADD((byte) 2),
        /** Scope: key + namespace, also affecting other (source) namespaces. */
        MERGE_NS((byte) 3),
        /** Scope: key + namespace + element (e.g. user map key put or list append). */
        CHANGE_ELEMENT((byte) 4),
        /** Scope: key + namespace + element (e.g. user map remove or iterator remove). */
        REMOVE_ELEMENT((byte) 5),
        /** Scope: key + namespace, last element. */
        POLL_ELEMENT((byte) 6),
        /** State metadata (name, serializers, etc.). */
        METADATA((byte) 7);
        private final byte code;

        StateChangeOperation(byte code) {
            this.code = code;
        }

        static Map<Byte, KvStateChangeLoggerImpl.StateChangeOperation> byCodes =
                Arrays.stream(AbstractStateChangeLogger.StateChangeOperation.values())
                        .collect(Collectors.toMap(o -> o.code, Function.identity()));

        public static StateChangeOperation byCode(byte opCode) {
            return checkNotNull(byCodes.get(opCode), Byte.toString(opCode));
        }
    }
}
