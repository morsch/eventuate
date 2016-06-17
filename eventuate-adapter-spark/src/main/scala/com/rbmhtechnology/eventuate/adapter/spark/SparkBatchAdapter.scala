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

package com.rbmhtechnology.eventuate.adapter.spark

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension

import com.datastax.spark.connector._
import com.datastax.spark.connector.types._
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log.cassandra.CassandraEventLogSettings
import com.typesafe.config._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class SparkBatchAdapter(context: SparkContext, config: Config) {
  private val cassandraSettings =
    new CassandraEventLogSettings(config)

  private implicit val converter: DurableEventConverter =
    new DurableEventConverter(config)

  TypeConverter.registerConverter(converter)

  def eventLog(logId: String, fromSequenceNr: Long = 1L): RDD[DurableEvent] = {
    context.cassandraTable(cassandraSettings.keyspace, s"${cassandraSettings.tablePrefix}_$logId")
      .select("event").where(s"sequence_nr >= $fromSequenceNr").as((event: DurableEvent) => event)
  }
}

private class DurableEventConverter(config: Config) extends TypeConverter[DurableEvent] {
  import scala.reflect.runtime.universe._

  val converter = implicitly[TypeConverter[Array[Byte]]]

  // --------------------------------------
  //  FIXME: how to shutdown actor system?
  // --------------------------------------

  @transient lazy val system = ActorSystem("TypeConverter", config)
  @transient lazy val serial = SerializationExtension(system)

  def targetTypeTag = implicitly[TypeTag[DurableEvent]]
  def convertPF = {
    case obj => deserialize(converter.convert(obj))
  }

  def deserialize(bytes: Array[Byte]): DurableEvent =
    serial.deserialize(bytes, classOf[DurableEvent]).get
}
