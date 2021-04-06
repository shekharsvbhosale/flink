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

import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;

class ChangelogApplierFactoryImpl implements ChangelogApplierFactory {
    @Override
    public <K, N, UK, UV> StateChangeLogReader.ChangeApplier<K, N> forMap(
            InternalMapState<K, N, UK, UV> map, InternalKeyContext<K> keyContext) {
        return new MapStateChangeLogApplier<>(map, keyContext);
    }

    @Override
    public <K, N, T> StateChangeLogReader.ChangeApplier<K, N> forList(
            InternalListState<K, N, T> state, InternalKeyContext<K> keyContext) {
        return new ListChangeApplier<>(keyContext, state);
    }

    @Override
    public <K, N, T> StateChangeLogReader.ChangeApplier<K, N> forReducing(
            InternalReducingState<K, N, T> state, InternalKeyContext<K> keyContext) {
        return new ReducingChangeApplier<>(keyContext, state);
    }

    @Override
    public <K, N, IN, SV, OUT> StateChangeLogReader.ChangeApplier<K, N> forAggregating(
            InternalAggregatingState<K, N, IN, SV, OUT> state, InternalKeyContext<K> keyContext) {
        return new AggregatingChangeApplier<>(keyContext, state);
    }

    @Override
    public <K, N, T> StateChangeLogReader.ChangeApplier<K, N> forValue(
            InternalValueState<K, N, T> state, InternalKeyContext<K> keyContext) {
        return new ValueChangeApplier<>(keyContext, state);
    }
}
