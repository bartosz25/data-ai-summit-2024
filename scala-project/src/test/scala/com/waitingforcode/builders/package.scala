package com.waitingforcode

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant

package object builders {

  def structTypeToColumnsSeq(schema: StructType): Seq[Column] = {
    schema.fieldNames.map(field => functions.col(field))
  }

  implicit def stringToTimestamp(datetime: String): Timestamp = {
    Timestamp.from(Instant.parse(datetime))
  }

  def nullableStringToOptionalTimestamp(nullableStringTimestamp: String): Option[Timestamp] = {
    Option(nullableStringTimestamp)
      .map(timestampString => Timestamp.from(Instant.parse(timestampString)))

  }

  val JsonMapper = new ObjectMapper()
  JsonMapper.registerModule(DefaultScalaModule)
  JsonMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"))
  JsonMapper.registerModule(new JavaTimeModule())

  val JsonMapperForSessions = new ObjectMapper()
  JsonMapperForSessions.registerModule(DefaultScalaModule)
  JsonMapperForSessions.registerModule(new JavaTimeModule())
  JsonMapperForSessions.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

}
