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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalValueState;

class ValueChangeApplier<K, N, T> implements StateChangeLogReader.ChangeApplier<K, N> {
    private final InternalKeyContext<K> keyContext;
    private final InternalValueState<K, N, T> state;

    public ValueChangeApplier(InternalKeyContext<K> keyContext, InternalValueState<K, N, T> state) {
        this.keyContext = keyContext;
        this.state = state;
    }

    @Override
    public void setKey(K k) {
        keyContext.setCurrentKey(k);
        keyContext.setCurrentKeyGroupIndex(
                KeyGroupRangeAssignment.assignToKeyGroup(k, keyContext.getNumberOfKeyGroups()));
    }

    @Override
    public void setNamespace(N n) {
        state.setCurrentNamespace(n);
    }

    @Override
    public void applyStateClear() {
        state.clear();
    }

    @Override
    public void applyStateUpdate(DataInputView in) throws Exception {
        state.update(state.getValueSerializer().deserialize(in));
    }

    @Override
    public void applyStateAdded(DataInputView in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void applyStateElementChanged(DataInputView in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void applyNameSpaceMerge(DataInputView in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void applyStateElementRemoved(DataInputView in) {
        throw new UnsupportedOperationException();
    }
}
