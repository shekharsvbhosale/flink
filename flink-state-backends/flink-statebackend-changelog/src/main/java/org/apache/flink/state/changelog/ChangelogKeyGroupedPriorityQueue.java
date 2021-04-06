/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static org.apache.flink.state.changelog.StateChangeLogger.loggingIterator;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link KeyGroupedInternalPriorityQueue} that keeps state on the underlying delegated {@link
 * KeyGroupedInternalPriorityQueue} as well as on the state change log.
 */
public class ChangelogKeyGroupedPriorityQueue<T> implements KeyGroupedInternalPriorityQueue<T> {
    private final KeyGroupedInternalPriorityQueue<T> delegatedPriorityQueue;
    private final PqStateChangeLogger<T> logger;
    private final TypeSerializer<T> serializer;

    public ChangelogKeyGroupedPriorityQueue(
            KeyGroupedInternalPriorityQueue<T> delegatedPriorityQueue,
            PqStateChangeLogger<T> logger,
            TypeSerializer<T> serializer) {
        this.delegatedPriorityQueue = checkNotNull(delegatedPriorityQueue);
        this.logger = checkNotNull(logger);
        this.serializer = serializer;
    }

    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        return delegatedPriorityQueue.getSubsetForKeyGroup(keyGroupId);
    }

    @Nullable
    @Override
    public T poll() {
        try {
            logger.stateElementPolled();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        return delegatedPriorityQueue.poll();
    }

    @Nullable
    @Override
    public T peek() {
        return delegatedPriorityQueue.peek();
    }

    @Override
    public boolean add(T toAdd) {
        try {
            logger.stateElementChanged(out -> serializer.serialize(toAdd, out), null);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        return delegatedPriorityQueue.add(toAdd);
    }

    @Override
    public boolean remove(T toRemove) {
        try {
            logger.stateElementRemoved(out -> serializer.serialize(toRemove, out), null);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        return delegatedPriorityQueue.remove(toRemove);
    }

    @Override
    public boolean isEmpty() {
        return delegatedPriorityQueue.isEmpty();
    }

    @Override
    public int size() {
        return delegatedPriorityQueue.size();
    }

    @Override
    public void addAll(@Nullable Collection<? extends T> toAdd) {
        if (toAdd == null) {
            return;
        }
        try {
            logger.stateElementChanged(
                    out -> {
                        for (T x : toAdd) {
                            serializer.serialize(x, out);
                        }
                    },
                    null);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        delegatedPriorityQueue.addAll(toAdd);
    }

    @Override
    public CloseableIterator<T> iterator() {
        return CloseableIterator.adapterForIterator(
                loggingIterator(
                        delegatedPriorityQueue.iterator(), logger, serializer::serialize, null));
    }
}
