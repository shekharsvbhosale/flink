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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.junit.Assert.assertEquals;

/** Tests for window join operators created by {@link WindowJoinOperatorBuilder}. */
public class WindowJoinOperatorTest {

    private InternalTypeInfo<RowData> rowType =
            InternalTypeInfo.ofFields(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));

    private InternalTypeInfo<RowData> outputRowType =
            InternalTypeInfo.ofFields(
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH));
    private RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(outputRowType.toRowFieldTypes());

    private RowDataHarnessAssertor semiAntiJoinAssertor =
            new RowDataHarnessAssertor(rowType.toRowFieldTypes());

    private String funcCode =
            "public class TestWindowJoinCondition extends org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "\n"
                    + "    public TestWindowJoinCondition(Object[] reference) {\n"
                    + "    }\n"
                    + "\n"
                    + "    @Override\n"
                    + "    public boolean apply(org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {\n"
                    + "        return true;\n"
                    + "    }\n"
                    + "}\n";

    private GeneratedJoinCondition joinFunction =
            new GeneratedJoinCondition("TestWindowJoinCondition", funcCode, new Object[0]);

    private int keyIdx = 1;
    private RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {keyIdx}, rowType.toRowFieldTypes());
    private TypeInformation<RowData> keyType = InternalTypeInfo.ofFields();

    @Test
    public void testSemiJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.SEMI);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(1L, "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement2(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(6L, "k1"));
        testHarness.processElement2(insertRecord(9L, "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(3L, "k1"));
        expectedOutput.add(insertRecord(3L, "k1"));
        expectedOutput.add(new Watermark(10));
        semiAntiJoinAssertor.assertOutputEqualsSorted(
                "output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(12L, "k1"));
        testHarness.processElement1(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(insertRecord(15L, "k1"));
        expectedOutput.add(new Watermark(18));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testAntiJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.ANTI);
        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(1L, "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement2(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(6L, "k1"));
        testHarness.processElement2(insertRecord(9L, "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(6L, "k1"));
        expectedOutput.add(new Watermark(10));
        semiAntiJoinAssertor.assertOutputEqualsSorted(
                "output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(12L, "k1"));
        testHarness.processElement1(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(insertRecord(12L, "k1"));
        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(new Watermark(18));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testInnerJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.INNER);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(1L, "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement2(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(6L, "k1"));
        testHarness.processElement2(insertRecord(9L, "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(3L, "k1", 3L, "k1"));
        expectedOutput.add(insertRecord(3L, "k1", 3L, "k1"));
        expectedOutput.add(new Watermark(10));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(12L, "k1"));
        testHarness.processElement1(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(insertRecord(15L, "k1", 15L, "k1"));
        expectedOutput.add(insertRecord(15L, "k1", 15L, "k1"));
        expectedOutput.add(new Watermark(18));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.LEFT);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(1L, "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement2(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(6L, "k1"));
        testHarness.processElement2(insertRecord(9L, "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(3L, "k1", 3L, "k1"));
        expectedOutput.add(insertRecord(3L, "k1", 3L, "k1"));
        expectedOutput.add(insertRecord(6L, "k1", null, null));
        expectedOutput.add(new Watermark(10));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(12L, "k1"));
        testHarness.processElement1(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(insertRecord(12L, "k1", null, null));
        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(insertRecord(15L, "k1", 15L, "k1"));
        expectedOutput.add(insertRecord(15L, "k1", 15L, "k1"));
        expectedOutput.add(new Watermark(18));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testRightOuterJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.RIGHT);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(1L, "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement2(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(6L, "k1"));
        testHarness.processElement2(insertRecord(9L, "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(3L, "k1", 3L, "k1"));
        expectedOutput.add(insertRecord(3L, "k1", 3L, "k1"));
        expectedOutput.add(insertRecord(null, null, 9L, "k1"));
        expectedOutput.add(new Watermark(10));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(12L, "k1"));
        testHarness.processElement1(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(insertRecord(15L, "k1", 15L, "k1"));
        expectedOutput.add(insertRecord(15L, "k1", 15L, "k1"));
        expectedOutput.add(new Watermark(18));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testOuterJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.FULL);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(1L, "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(3L, "k1"));
        testHarness.processElement2(insertRecord(3L, "k1"));
        testHarness.processElement1(insertRecord(6L, "k1"));
        testHarness.processElement2(insertRecord(9L, "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(3L, "k1", 3L, "k1"));
        expectedOutput.add(insertRecord(3L, "k1", 3L, "k1"));
        expectedOutput.add(insertRecord(6L, "k1", null, null));
        expectedOutput.add(insertRecord(null, null, 9L, "k1"));
        expectedOutput.add(new Watermark(10));
        assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(12L, "k1"));
        testHarness.processElement1(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        testHarness.processElement2(insertRecord(15L, "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(insertRecord(12L, "k1", null, null));
        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(insertRecord(15L, "k1", 15L, "k1"));
        expectedOutput.add(insertRecord(15L, "k1", 15L, "k1"));
        expectedOutput.add(new Watermark(18));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(FlinkJoinType joinType) throws Exception {
        WindowJoinOperator operator =
                WindowJoinOperatorBuilder.builder()
                        .leftType(rowType)
                        .rightType(rowType)
                        .generatedJoinCondition(joinFunction)
                        .leftWindowEndIndex(0)
                        .rightWindowEndIndex(0)
                        .filterNullKeys(new boolean[] {true})
                        .joinType(joinType)
                        .build();
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector, keyType);
        return testHarness;
    }
}
