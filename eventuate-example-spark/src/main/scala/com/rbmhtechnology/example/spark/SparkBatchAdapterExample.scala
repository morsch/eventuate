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

import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.DurableEvent

import com.rbmhtechnology.eventuate.adapter.spark.SparkBatchAdapter
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.log.cassandra.{ CassandraEventLogSettings, CassandraEventLog }

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, DataFrame, SQLContext }
import org.apache.spark.{ SparkContext, SparkConf }

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._

case class DomainEvent(sequenceNr: Long, payload: String)

object SparkBatchAdapterExample extends App {

  // ---------------------------------------------------------------
  //  Assumption: Cassandra 2.1 or higher running on localhost:9042
  // ---------------------------------------------------------------

  implicit val system = ActorSystem("spark-example")

  val cassandraSettings = new CassandraEventLogSettings(system.settings.config)
  val sparkConfig = new SparkConf(true)
    .set("spark.cassandra.connection.host", cassandraSettings.contactPoints.head.getHostName)
    .set("spark.cassandra.connection.port", cassandraSettings.contactPoints.head.getPort.toString)
    .set("spark.cassandra.auth.username", system.settings.config.getString("eventuate.log.cassandra.username"))
    .set("spark.cassandra.auth.password", system.settings.config.getString("eventuate.log.cassandra.password"))
    .setAppName("adapter")
    .setMaster("local[4]")

  val logId = "example"
  val log = system.actorOf(CassandraEventLog.props("example"))

  // Write some events to event log
  Await.result(new EventLogWriter("writer", log).write(Seq("a", "b", "c", "d", "e")), 20.seconds)

  val sparkContext: SparkContext = new SparkContext(sparkConfig)
  val sqlContext: SQLContext = new SQLContext(sparkContext)

  // Create an Eventuate Spark batch adapter
  val sparkBatchAdapter: SparkBatchAdapter = new SparkBatchAdapter(sparkContext, system.settings.config)

  import sqlContext.implicits._

  // Fetch all events from given event log as Spark RDD
  val events: RDD[DurableEvent] = sparkBatchAdapter.eventLog(logId)

  // Fetch events starting at sequence number 3 from given event log as Spark RDD
  val eventsFrom: RDD[DurableEvent] = sparkBatchAdapter.eventLog(logId, fromSequenceNr = 3L)

  // By default, events are sorted by sequence number *per partition*.
  // Use .sortBy(_.localSequenceNr) to create a totally ordered RDD.
  val eventsSorted: RDD[DurableEvent] = events.sortBy(_.localSequenceNr)

  // Create a custom DataFrame from RDD[DurableEvent]
  val eventsDF: DataFrame = events.map(event => DomainEvent(event.localSequenceNr, event.payload.toString)).toDF()
  // Obtaining DataFrames directly will be supported in a later version

  // Create a custom Dataset from RDD[DurableEvent]
  val eventDS: Dataset[DomainEvent] = events.map(event => DomainEvent(event.localSequenceNr, event.payload.toString)).toDS()
  // Obtaining DataSets directly will be supported in a later version

  // Write sorted events to stdout
  eventsSorted.collect().foreach(println)

  // Sorted DataFrame by sequenceNr and write to stdout
  eventsDF.orderBy("sequenceNr").collect().foreach(println)

  sparkContext.stop()
  system.terminate()
}
