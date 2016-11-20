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

import scala.concurrent.duration.Duration

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}


class EarlyStopLimitRpcEndpoint(val sparkContext: SparkContext) extends RpcEndpoint{

  var globalRowCount = 0L

  override val rpcEnv: RpcEnv = sparkContext.env.rpcEnv

  override def receive: PartialFunction[Any, Unit] = {
    case x: Long =>
      globalRowCount = globalRowCount + x
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case x: Long =>
      globalRowCount = globalRowCount + x
      context.reply(globalRowCount)
  }
}

object EarlyStopLimitRpcEndpoint{

  def getEndpointRef(endpointName: String): RpcEndpointRef = {
    RpcUtils.makeDriverRef(endpointName, SparkEnv.get.conf, SparkEnv.get.rpcEnv)
  }

  def send(endpointName: String, count: Long): Unit = {
    getEndpointRef(endpointName).send(count)
  }

  def ask(endpointName: String): java.lang.Long = {
    val future = getEndpointRef(endpointName).ask[Long](0L)
    java.lang.Long.valueOf(ThreadUtils.awaitResult[Long](future, Duration.Inf))
  }
}


