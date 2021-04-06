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

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoReader;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.StateTypeHint.KEYED_STATE;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.METADATA;
import static org.apache.flink.state.changelog.FunctionDelegationHelper.delegateAggregateFunction;
import static org.apache.flink.state.changelog.FunctionDelegationHelper.delegateReduceFunction;
import static org.apache.flink.util.Preconditions.checkNotNull;

@SuppressWarnings({"rawtypes", "unchecked"})
class StateChangeLogReaderImpl implements StateChangeLogReader {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeLogReaderImpl.class);

    @Override
    public void readAndApply(
            DataInputView in,
            int keyGroup,
            ChangelogKeyedStateBackend<?, ?> backend,
            ClassLoader classLoader)
            throws Exception {
        ChangelogApplierFactory factory = new ChangelogApplierFactoryImpl();
        StateChangeOperation operation = StateChangeOperation.byCode(in.readByte());
        LOG.debug("apply {} in key group {}", operation, keyGroup);
        if (operation == METADATA) {
            restoreMetaData(in, backend, classLoader);
        } else if (backend.getKeyGroupRange().contains(keyGroup)) {
            stateChange(in, factory, backend, operation);
        }
    }

    private void restoreMetaData(
            DataInputView in, ChangelogKeyedStateBackend<?, ?> backend, ClassLoader classLoader)
            throws Exception {
        StateMetaInfoSnapshot snapshot = readStateMetaInfoSnapshot(in, classLoader);
        switch (snapshot.getBackendStateType()) {
            case KEY_VALUE:
                restoreKvMetaData(backend, snapshot);
                return;
            case PRIORITY_QUEUE:
                restorePqMetaData(backend, snapshot);
                return;
            default:
                throw new RuntimeException(
                        "Unsupported state type: "
                                + snapshot.getBackendStateType()
                                + ", sate: "
                                + snapshot.getName());
        }
    }

    private void restoreKvMetaData(
            ChangelogKeyedStateBackend<?, ?> backend, StateMetaInfoSnapshot snapshot)
            throws Exception {
        RegisteredKeyValueStateBackendMetaInfo meta =
                new RegisteredKeyValueStateBackendMetaInfo(snapshot);
        // Use regular API to create states in both changelog and the base backends the metadata is
        // persisted in log before data changes.
        // An alternative solution to load metadata "natively" by the base backends would require
        // base state to be always present, i.e. the 1st checkpoint would have to be "full" always.
        backend.getOrCreateKeyedState(meta.getNamespaceSerializer(), toStateDescriptor(meta));
    }

    private StateDescriptor toStateDescriptor(RegisteredKeyValueStateBackendMetaInfo meta) {
        switch (meta.getStateType()) {
            case VALUE:
                return new ValueStateDescriptor(meta.getName(), meta.getStateSerializer());
            case MAP:
                MapSerializer mapSerializer = (MapSerializer) meta.getStateSerializer();
                return new MapStateDescriptor(
                        meta.getName(),
                        mapSerializer.getKeySerializer(),
                        mapSerializer.getValueSerializer());
            case LIST:
                return new ListStateDescriptor(
                        meta.getName(),
                        ((ListSerializer) meta.getStateSerializer()).getElementSerializer());
            case AGGREGATING:
                return new AggregatingStateDescriptor(
                        meta.getName(), delegateAggregateFunction(), meta.getStateSerializer());
            case REDUCING:
                return new ReducingStateDescriptor(
                        meta.getName(), delegateReduceFunction(), meta.getStateSerializer());
            default:
                throw new IllegalArgumentException(meta.getStateType().toString());
        }
    }

    private void restorePqMetaData(
            ChangelogKeyedStateBackend<?, ?> backend, StateMetaInfoSnapshot snapshot) {
        RegisteredPriorityQueueStateBackendMetaInfo meta =
                new RegisteredPriorityQueueStateBackendMetaInfo(snapshot);
        backend.create(meta.getName(), meta.getElementSerializer());
    }

    private StateMetaInfoSnapshot readStateMetaInfoSnapshot(
            DataInputView in, ClassLoader classLoader) throws IOException {
        int version = in.readInt();
        StateMetaInfoReader reader =
                StateMetaInfoSnapshotReadersWriters.getReader(version, KEYED_STATE);
        return reader.readStateMetaInfoSnapshot(in, classLoader);
    }

    private <K, N> void stateChange(
            DataInputView in,
            ChangelogApplierFactory factory,
            ChangelogKeyedStateBackend<?, ?> backend,
            StateChangeOperation operation)
            throws Exception {
        String stateName = checkNotNull(in.readUTF());
        AbstractChangelogState<K, N, ?, ?> state =
                checkNotNull(backend.getStateById(stateName), "state %s not found", stateName);
        ChangeApplier<K, N> changeApplier = state.getChangeApplier(factory);
        changeApplier.setKey(state.getKeySerializer().deserialize(in));
        changeApplier.setNamespace(state.getNamespaceSerializer().deserialize(in));
        switch (operation) {
            case ADD:
                changeApplier.applyStateAdded(in);
                return;
            case CLEAR:
                changeApplier.applyStateClear();
                return;
            case CHANGE_ELEMENT:
                changeApplier.applyStateElementChanged(in);
                return;
            case SET:
                changeApplier.applyStateUpdate(in);
                return;
            case MERGE_NS:
                changeApplier.applyNameSpaceMerge(in);
                return;
            case REMOVE_ELEMENT:
                changeApplier.applyStateElementRemoved(in);
                return;
            default:
                throw new IllegalArgumentException("Unknown state change operation: " + operation);
        }
    }
}
