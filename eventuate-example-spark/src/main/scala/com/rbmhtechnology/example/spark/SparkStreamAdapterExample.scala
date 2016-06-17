/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.example.spark

import akka.actor._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.adapter.spark.SparkStreamAdapter
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

import org.apache.spark._
import org.apache.spark.streaming._

import scala.collection.immutable._
import scala.io.Source

object SparkStreamAdapterExample extends App {
  implicit val system: ActorSystem = ActorSystem(ReplicationConnection.DefaultRemoteSystemName)

  val logName: String = "L"
  val endpoint: ReplicationEndpoint = new ReplicationEndpoint(id = "1", logNames = Set(logName), logFactory = logId => LeveldbEventLog.props(logId), connections = Set())
  val log: ActorRef = endpoint.logs(logName)
  val writer: EventLogWriter = new EventLogWriter("writer", log)

  endpoint.activate()

  val sparkConfig = new SparkConf(true).setAppName("adapter").setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConfig)
  val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))

  // Create an Eventuate Spark stream adapter
  val sparkStreamAdapter = new SparkStreamAdapter(sparkStreamingContext, system.settings.config)

  // Create a DStream from the event log by connecting to its replication endpoint
  val stream = sparkStreamAdapter.eventStream("s1", "127.0.0.1", 2552, logName, fromSequenceNr = 1L)
  // In a later version, Eventuate will internally store the processing progress
  // so that applications don't need to pass a fromSequenceNr to eventStream(...)

  // Process the event stream by printing events to stdout
  stream.foreachRDD(rdd => rdd.foreach(println))

  // For a processing order equal to storage order in the event log, use repartition(1)
  //stream.repartition(1).foreachRDD(rdd => rdd.foreach(println))

  // Start event stream processing
  sparkStreamingContext.start()

  // Generate new events from stdin
  val lines = Source.stdin.getLines()
  while (lines.hasNext) writer.write(Seq(lines.next()))
}
