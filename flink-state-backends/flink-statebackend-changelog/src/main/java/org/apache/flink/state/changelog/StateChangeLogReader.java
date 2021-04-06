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

interface StateChangeLogReader {

    void readAndApply(
            DataInputView in, int keyGroup, ChangelogKeyedStateBackend<?, ?> backend, ClassLoader ucl)
            throws Exception;

    interface ChangeApplier<Key, Namespace> {
        void setKey(Key key);

        void setNamespace(Namespace namespace);

        void applyStateClear();

        void applyStateUpdate(DataInputView in) throws Exception;

        void applyStateAdded(DataInputView in) throws Exception;

        void applyStateElementChanged(DataInputView in) throws Exception;

        void applyNameSpaceMerge(DataInputView in) throws Exception;

        void applyStateElementRemoved(DataInputView in) throws Exception;
    }
}
