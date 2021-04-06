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

package org.apache.flink.table.runtime.operators.join.window;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link WindowJoinOperatorBuilder} is used to build a {@link WindowJoinOperator} for window
 * join.
 *
 * <pre>
 * WindowJoinOperatorBuilder.builder()
 *   .leftType(leftType)
 *   .rightType(rightType)
 *   .generatedJoinCondition(generatedJoinCondition)
 *   .leftWindowEndIndex(leftWindowEndIndex)
 *   .rightWindowEndIndex(rightWindowEndIndex)
 *   .filterNullKeys(filterNullKeys)
 *   .joinType(joinType)
 *   .build();
 * </pre>
 */
public class WindowJoinOperatorBuilder {

    public static WindowJoinOperatorBuilder builder() {
        return new WindowJoinOperatorBuilder();
    }

    private InternalTypeInfo<RowData> leftType;
    private InternalTypeInfo<RowData> rightType;
    private GeneratedJoinCondition generatedJoinCondition;
    private int leftWindowEndIndex = -1;
    private int rightWindowEndIndex = -1;
    private boolean[] filterNullKeys;
    private FlinkJoinType joinType;

    public WindowJoinOperatorBuilder leftType(InternalTypeInfo<RowData> leftType) {
        this.leftType = leftType;
        return this;
    }

    public WindowJoinOperatorBuilder rightType(InternalTypeInfo<RowData> rightType) {
        this.rightType = rightType;
        return this;
    }

    public WindowJoinOperatorBuilder generatedJoinCondition(
            GeneratedJoinCondition generatedJoinCondition) {
        this.generatedJoinCondition = generatedJoinCondition;
        return this;
    }

    public WindowJoinOperatorBuilder filterNullKeys(boolean[] filterNullKeys) {
        this.filterNullKeys = filterNullKeys;
        return this;
    }

    public WindowJoinOperatorBuilder joinType(FlinkJoinType joinType) {
        this.joinType = joinType;
        return this;
    }

    public WindowJoinOperatorBuilder leftWindowEndIndex(int leftWindowEndIndex) {
        this.leftWindowEndIndex = leftWindowEndIndex;
        return this;
    }

    public WindowJoinOperatorBuilder rightWindowEndIndex(int rightWindowEndIndex) {
        this.rightWindowEndIndex = rightWindowEndIndex;
        return this;
    }

    public WindowJoinOperator build() {
        checkNotNull(leftType);
        checkNotNull(rightType);
        checkNotNull(generatedJoinCondition);
        checkNotNull(filterNullKeys);
        checkNotNull(joinType);

        checkArgument(
                leftWindowEndIndex >= 0,
                String.format(
                        "Illegal window end index %s, it should not be negative!",
                        leftWindowEndIndex));
        checkArgument(
                rightWindowEndIndex >= 0,
                String.format(
                        "Illegal window end index %s, it should not be negative!",
                        rightWindowEndIndex));

        switch (joinType) {
            case INNER:
                return new WindowJoinOperator.InnerJoinOperator(
                        leftType,
                        rightType,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys);
            case SEMI:
                return new WindowJoinOperator.SemiAntiJoinOperator(
                        leftType,
                        rightType,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys,
                        false);
            case ANTI:
                return new WindowJoinOperator.SemiAntiJoinOperator(
                        leftType,
                        rightType,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys,
                        true);
            case LEFT:
                return new WindowJoinOperator.LeftOuterJoinOperator(
                        leftType,
                        rightType,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys);
            case RIGHT:
                return new WindowJoinOperator.RightOuterJoinOperator(
                        leftType,
                        rightType,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys);
            case FULL:
                return new WindowJoinOperator.FullOuterJoinOperator(
                        leftType,
                        rightType,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys);
            default:
                throw new IllegalArgumentException("Invalid join type: " + joinType);
        }
    }
}