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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.{FailingCollectionSource, StreamingWithStateTestBase, TestData, TestingAppendSink}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Ignore, Test}

@RunWith(classOf[Parameterized])
class WindowJoinITCase(mode: StateBackendMode)
  extends StreamingWithStateTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    // enable checkpoint, we are using failing source to force have a complete checkpoint
    // and cover restore path
    env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    FailingCollectionSource.reset()

    val dataId1 = TestValuesTableFactory.registerData(TestData.windowData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE T1 (
         | `ts` STRING,
         | `int` INT,
         | `double` DOUBLE,
         | `float` FLOAT,
         | `bigdec` DECIMAL(10, 2),
         | `string` STRING,
         | `name` STRING,
         | `rowtime` AS TO_TIMESTAMP(`ts`),
         | WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
         |) WITH (
         | 'connector' = 'values',
         | 'data-id' = '$dataId1',
         | 'failing-source' = 'true'
         |)
         |""".stripMargin)

    val dataId2 = TestValuesTableFactory.registerData(TestData.windowData2)
    tEnv.executeSql(
      s"""
         |CREATE TABLE T2 (
         | `ts` STRING,
         | `int` INT,
         | `double` DOUBLE,
         | `float` FLOAT,
         | `bigdec` DECIMAL(10, 2),
         | `string` STRING,
         | `name` STRING,
         | `rowtime` AS TO_TIMESTAMP(`ts`),
         | WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
         |) WITH (
         | 'connector' = 'values',
         | 'data-id' = '$dataId2',
         | 'failing-source' = 'true'
         |)
         |""".stripMargin)
  }

  @Test
  def testInnerJoin(): Unit = {
    val sql =
      """
        |SELECT L.`name`, L.window_start, L.window_end, uv1, uv2
        |FROM (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv1
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) L
        |JOIN (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv2
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.`name` = R.`name`
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore("After [FLINK-22098] is merged")
  @Test
  def testInnerJoinWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT L.`name`, L.window_start, L.window_end, uv1, uv2
        |FROM (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv1
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) L
        |JOIN (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv2
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND
        |L.`name` IS NOT DISTINCT from R.`name`
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,0,0")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore("After [FLINK-22099] is merged")
  @Test
  def testSemiJoinExists(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv1
        |    FROM TABLE(
        |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |) L WHERE EXISTS (
        |SELECT * FROM(
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv2
        |    FROM TABLE(
        |      TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |) R
        |  WHERE L.window_start = R.window_start AND L.window_end = R.window_end
        |        AND L.`name` = R.`name`)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore(
    "1. After [FLINK-22099] is merged " +
      "2.FIXME RelDecorrelator throws exception in the case")
  @Test
  def testSemiJoinExistsWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv1
        |    FROM TABLE(
        |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |) L WHERE EXISTS (
        |SELECT * FROM(
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv2
        |    FROM TABLE(
        |      TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |) R
        |  WHERE L.window_start = R.window_start AND L.window_end = R.window_end
        |        AND L.`name` IS NOT DISTINCT from R.`name`)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore("After [FLINK-22099] is merged")
  @Test
  def testSemiJoinIN(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv1
        |    FROM TABLE(
        |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |) L WHERE L.`name` IN (
        |SELECT `name` FROM(
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv2
        |    FROM TABLE(
        |      TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |) R
        |  WHERE L.window_start = R.window_start AND L.window_end = R.window_end)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore(
    "1. After [FLINK-22099] is merged " +
      "2.FIXME RelDecorrelator throws exception in the case")
  @Test
  def testSemiJoinINWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv1
        |    FROM TABLE(
        |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |) L WHERE L.window_start IN (
        |SELECT `window_start` FROM(
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv2
        |    FROM TABLE(
        |      TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |) R
        |  WHERE L.window_end = R.window_end AND
        |        L.`name` IS NOT DISTINCT from R.`name`)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore("After [FLINK-22099] is merged")
  @Test
  def testAntiJoinNotExists(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv1
        |    FROM TABLE(
        |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |) L WHERE NOT EXISTS (
        |SELECT * FROM(
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv2
        |    FROM TABLE(
        |      TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |) R
        |  WHERE L.window_start = R.window_start AND L.window_end = R.window_end
        |        AND L.`name` = R.`name`)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,2",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,0")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore(
    "1. After [FLINK-22099] is merged " +
      "2. After [FLINK-22098] is merged " +
      "3.FIXME RelDecorrelator throws exception in the case")
  @Test
  def testAntiJoinNotExistsWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv1
        |    FROM TABLE(
        |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |) L WHERE NOT EXISTS (
        |SELECT * FROM(
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv2
        |    FROM TABLE(
        |      TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |) R
        |  WHERE L.window_start = R.window_start AND L.window_end = R.window_end
        |        AND L.`name` IS NOT DISTINCT from R.`name`)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,2",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore("After [FLINK-22099] is merged")
  @Test
  def testAntiJoinNotIN(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv1
        |    FROM TABLE(
        |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |) L WHERE L.`name` NOT IN (
        |SELECT `name` FROM(
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv2
        |    FROM TABLE(
        |      TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |) R
        |  WHERE L.window_start = R.window_start AND L.window_end = R.window_end)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,2",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore(
    "1. After [FLINK-22099] is merged " +
      "2. After [FLINK-22098] is merged " +
      "3.FIXME RelDecorrelator throws exception in the case")
  @Test
  def testAntiJoinNotINWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT * FROM (
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv1
        |    FROM TABLE(
        |    TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |  GROUP BY `name`, window_start, window_end
        |) L WHERE L.window_start NOT IN (
        |SELECT `window_start` FROM(
        |  SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) as uv2
        |    FROM TABLE(
        |      TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |    GROUP BY `name`, window_start, window_end
        |) R
        |  WHERE L.window_end = R.window_end AND
        |        L.`name` IS NOT DISTINCT from R.`name`)
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,2",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testLeftJoin(): Unit = {
    val sql =
      """
        |SELECT L.`name`, L.window_start, L.window_end, uv1, uv2
        |FROM (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv1
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) L
        |LEFT JOIN (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv2
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.`name` = R.`name`
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,2,null",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,null",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore("After [FLINK-22098] is merged")
  @Test
  def testLeftJoinWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT L.`name`, L.window_start, L.window_end, uv1, uv2
        |FROM (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv1
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) L
        |LEFT JOIN (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv2
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND
        |   L.`name` IS NOT DISTINCT from R.`name`
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,2,null",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,null",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,0,0")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testRightJoin(): Unit = {
    val sql =
      """
        |SELECT L.`name`, R.window_start, R.window_end, uv1, uv2
        |FROM (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv1
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) L
        |RIGHT JOIN (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv2
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.`name` = R.`name`
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "null,2020-10-10T00:00,2020-10-10T00:00:05,null,2",
      "null,2020-10-10T00:00:05,2020-10-10T00:00:10,null,1",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,null,0")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore("After [FLINK-22098] is merged")
  @Test
  def testRightJoinWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT L.`name`, R.window_start, R.window_end, uv1, uv2
        |FROM (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv1
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) L
        |RIGHT JOIN (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv2
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND
        |   L.`name` IS NOT DISTINCT from R.`name`
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "null,2020-10-10T00:00,2020-10-10T00:00:05,null,2",
      "null,2020-10-10T00:00:05,2020-10-10T00:00:10,null,1",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,0,0")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testOuterJoin(): Unit = {
    val sql =
      """
        |SELECT L.`name`, L.window_start, L.window_end, R.`name`, R.window_start, R.window_end,
        |uv1, uv2
        |FROM (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv1
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) L
        |FULL OUTER JOIN (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv2
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.`name` = R.`name`
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,null,null,null,2,null",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,null,null,null,1,null",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,null,null,null,0,null",
      "null,null,null,a1,2020-10-10T00:00,2020-10-10T00:00:05,null,2",
      "null,null,null,a1,2020-10-10T00:00:05,2020-10-10T00:00:10,null,1",
      "null,null,null,null,2020-10-10T00:00:30,2020-10-10T00:00:35,null,0")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Ignore("After [FLINK-22098] is merged")
  @Test
  def testOuterJoinWithIsNotDistinctFrom(): Unit = {
    val sql =
      """
        |SELECT L.`name`, L.window_start, L.window_end, R.`name`, R.window_start, R.window_end,
        |uv1, uv2
        |FROM (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv1
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) L
        |FULL OUTER JOIN (
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(DISTINCT `string`) as uv2
        |FROM TABLE(
        |   TUMBLE(TABLE T2, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND
        |   L.`name` IS NOT DISTINCT from R.`name`
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,null,null,null,2,null",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,null,null,null,1,null",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,2",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,1",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,1",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,null,2020-10-10T00:00:30," +
        "2020-10-10T00:00:35,0,0",
      "null,null,null,a1,2020-10-10T00:00,2020-10-10T00:00:05,null,2",
      "null,null,null,a1,2020-10-10T00:00:05,2020-10-10T00:00:10,null,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }
}
