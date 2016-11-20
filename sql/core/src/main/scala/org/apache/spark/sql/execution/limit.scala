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

package org.apache.spark.sql.execution

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, LazilyGeneratedOrdering}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.Utils


/**
 * Take the first `limit` elements and collect them to a single partition.
 *
 * This operator will be used when a logical `Limit` operation is the final operator in an
 * logical plan, which happens when the user is collecting results back to the driver.
 */
case class CollectLimitExec(limit: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition
  override def executeCollect(): Array[InternalRow] = child.executeTake(limit)
  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)
  protected override def doExecute(): RDD[InternalRow] = {
    val shuffled = new ShuffledRowRDD(
      ShuffleExchange.prepareShuffleDependency(
        child.execute(), child.output, SinglePartition, serializer))
    shuffled.mapPartitionsInternal(_.take(limit))
  }
}

/**
 * Helper trait which defines methods that are shared by both
 * [[LocalLimitExec]] and [[GlobalLimitExec]].
 */
trait BaseLimitExec extends UnaryExecNode with CodegenSupport {
  val limit: Int
  override def output: Seq[Attribute] = child.output
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning
  protected override def doExecute(): RDD[InternalRow] = child.execute().mapPartitions { iter =>
    iter.take(limit)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val stopEarly = ctx.freshName("stopEarly")
    ctx.addMutableState("boolean", stopEarly, s"$stopEarly = false;")

    ctx.addNewFunction("shouldStop", s"""
      @Override
      protected boolean shouldStop() {
        return !currentRows.isEmpty() || $stopEarly;
      }
    """)
    val countTerm = ctx.freshName("count")
    ctx.addMutableState("int", countTerm, s"$countTerm = 0;")
    s"""
       | if ($countTerm < $limit) {
       |   $countTerm += 1;
       |   ${consume(ctx, input)}
       | } else {
       |   $stopEarly = true;
       | }
     """.stripMargin
  }
}

/**
 * Take the first `limit` elements of each child partition, but do not collect or shuffle them.
 */
case class LocalLimitExec(limit: Int, child: SparkPlan) extends BaseLimitExec {
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

/**
 * Take the first `limit` elements of the child's single output partition.
 */
case class GlobalLimitExec(limit: Int, child: SparkPlan) extends BaseLimitExec {
  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil
}

/**
 * Take the first limit elements as defined by the sortOrder, and do projection if needed.
 * This is logically equivalent to having a Limit operator after a [[SortExec]] operator,
 * or having a [[ProjectExec]] operator between them.
 * This could have been named TopK, but Spark's top operator does the opposite in ordering
 * so we name it TakeOrdered to avoid confusion.
 */
case class TakeOrderedAndProjectExec(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Option[Seq[NamedExpression]],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = {
    projectList.map(_.map(_.toAttribute)).getOrElse(child.output)
  }

  override def outputPartitioning: Partitioning = SinglePartition

  override def executeCollect(): Array[InternalRow] = {
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val data = child.execute().map(_.copy()).takeOrdered(limit)(ord)
    if (projectList.isDefined) {
      val proj = UnsafeProjection.create(projectList.get, child.output)
      data.map(r => proj(r).copy())
    } else {
      data
    }
  }

  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)

  protected override def doExecute(): RDD[InternalRow] = {
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val localTopK: RDD[InternalRow] = {
      child.execute().map(_.copy()).mapPartitions { iter =>
        org.apache.spark.util.collection.Utils.takeOrdered(iter, limit)(ord)
      }
    }
    val shuffled = new ShuffledRowRDD(
      ShuffleExchange.prepareShuffleDependency(
        localTopK, child.output, SinglePartition, serializer))
    shuffled.mapPartitions { iter =>
      val topK = org.apache.spark.util.collection.Utils.takeOrdered(iter.map(_.copy()), limit)(ord)
      if (projectList.isDefined) {
        val proj = UnsafeProjection.create(projectList.get, child.output)
        topK.map(r => proj(r))
      } else {
        topK
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def simpleString: String = {
    val orderByString = Utils.truncatedString(sortOrder, "[", ",", "]")
    val outputString = Utils.truncatedString(output, "[", ",", "]")

    s"TakeOrderedAndProject(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}

case class GlobalEarlyStopLimitExec(
    limit: Int,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {
  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  // There is no early stop in non-codegen
  protected override def doExecute(): RDD[InternalRow] = child.execute().mapPartitions { iter =>
    iter.take(limit)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // Variable Name for early stop
  var stopEarly: String = null
  var localCount: String = null
  var globalLimitCount: String = null

  protected override def doProduce(ctx: CodegenContext): String = {

    // A endPoint name for [EarlyStopLimitRpcEndpoint]
    val endPointName = "earlyStop_" + UUID.randomUUID()

    // register RpcEndpoint
    sparkContext.env.rpcEnv.setupEndpoint(endPointName, new EarlyStopLimitRpcEndpoint(sparkContext))

    stopEarly = ctx.freshName("stopEarly")
    ctx.addMutableState("boolean", stopEarly, s"$stopEarly = false;")

    localCount = ctx.freshName("count")
    ctx.addMutableState("long", localCount, s"$localCount = 0;")

    // global limit count. Get before while loop
    globalLimitCount = ctx.freshName("globalCount")
    ctx.addMutableState("long", globalLimitCount, s"$globalLimitCount = getGlobalLimitCount();")

    ctx.addNewFunction("getGlobalLimitCount",
      s"""
         | protected long getGlobalLimitCount() {
         |   long count = org.apache.spark.sql.execution.EarlyStopLimitRpcEndpoint
         |                    .ask("$endPointName");
         |   if(count >= $limit) {
         |     setStopEarly();
         |   }
         |   System.out.println("DEBUG: " + count);
         |   return 0L;
         | }
       """.stripMargin)

    ctx.addNewFunction("updateGlobalLimitCount",
      s"""
         | protected void updateGlobalLimitCount() {
         |    System.out.println("DEBUG: local count: " + $localCount +
         |                    "Global count: " + $globalLimitCount);
         |   org.apache.spark.sql.execution.EarlyStopLimitRpcEndpoint
         |                     .send("$endPointName", $localCount);
         | }
       """.stripMargin)

    // TODO: clean up Early stop clean up
    ctx.addNewFunction("setStopEarly",
      s"""
         | protected void setStopEarly() {
         |   System.out.println("Early Stop now");
         |   $stopEarly = true;
         | }
       """.stripMargin)

    s"""
       | if(!$stopEarly) {
       |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
       |   updateGlobalLimitCount();
       |  }else {
       |    return ;
       |  }
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {

    s"""
       | if ($localCount + $globalLimitCount < $limit) {
       |   $localCount += 1;
       |   ${consume(ctx, input)}
       | } else {
       |   setStopEarly();
       |   break;
       | }
     """.stripMargin
  }
}
