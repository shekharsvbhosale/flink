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

import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalMapState;

class MapStateChangeLogApplier<K, N, UK, UV> implements StateChangeLogReader.ChangeApplier<K, N> {
    private final InternalKeyContext<K> keyContext;
    private final InternalMapState<K, N, UK, UV> mapState;

    public MapStateChangeLogApplier(
            InternalMapState<K, N, UK, UV> mapState, InternalKeyContext<K> keyContext) {
        this.keyContext = keyContext;
        this.mapState = mapState;
    }

    @Override
    public void setKey(K k) {
        keyContext.setCurrentKey(k);
        keyContext.setCurrentKeyGroupIndex(
                KeyGroupRangeAssignment.assignToKeyGroup(k, keyContext.getNumberOfKeyGroups()));
    }

    @Override
    public void setNamespace(N n) {
        mapState.setCurrentNamespace(n);
    }

    @Override
    public void applyStateClear() {
        mapState.clear();
    }

    @Override
    public void applyStateUpdate(DataInputView in) throws Exception {
        mapState.putAll(getMapSerializer().deserialize(in));
    }

    @Override
    public void applyStateAdded(DataInputView in) throws Exception {
        mapState.putAll(getMapSerializer().deserialize(in));
    }

    @Override
    public void applyStateElementChanged(DataInputView in) throws Exception {
        UK k = getMapSerializer().getKeySerializer().deserialize(in);
        UV v = getMapSerializer().getValueSerializer().deserialize(in);
        mapState.put(k, v);
    }

    @Override
    public void applyNameSpaceMerge(DataInputView in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void applyStateElementRemoved(DataInputView in) throws Exception {
        mapState.remove(getMapSerializer().getKeySerializer().deserialize(in));
    }

    private MapSerializer<UK, UV> getMapSerializer() {
        return (MapSerializer<UK, UV>) mapState.getValueSerializer();
    }
}
