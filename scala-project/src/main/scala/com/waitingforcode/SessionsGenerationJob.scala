package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter
import scopt.OParser

import java.util.TimeZone

// --kafkaBootstrapServers localhost:9094 --kafkaInputTopic visits --kafkaOutputTopic sessions --devicesTableLocation /tmp/dais2024/devices --checkpointLocation /tmp/dais24_checkpoint/
object SessionsGenerationJob {

  def main(args: Array[String]): Unit = {
    OParser.parse(SessionsGenerationJobArguments.Parser, args,
      SessionsGenerationJobArguments()) match {
      case Some(jobConfig) => {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
        val sparkSession = SparkSession.builder()
          .master("local[*]")
          .config("spark.sql.session.timeZone", "UTC")
          .withExtensions(new DeltaSparkSessionExtension())
          .config("spark.sql.shuffle.partitions", 2)
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()

        val inputDataStream = sparkSession.readStream
          .options(Map(
            "kafka.bootstrap.servers" -> jobConfig.kafkaBootstrapServers,
            "subscribe" -> jobConfig.kafkaInputTopic,
            "maxOffsetsPerTrigger" -> "1000"
          )).format("kafka").load()

        DevicesToDeltaConverter.convertDevicesToDeltaTable(sparkSession, jobConfig.devicesTableLocation,
          jobConfig.devicesDeltaTableLocation)
        val devicesTable = sparkSession.read.format("delta").load(jobConfig.devicesDeltaTableLocation)

        val sessionsWriter: DataStreamWriter[Row] = SessionsGenerationJobLogic.generateSessions(
          inputDataStream, devicesTable, ProcessingTimeTrigger("10 seconds")
        )

        val writerQuery = sessionsWriter.options(Map(
          "checkpointLocation" -> jobConfig.checkpointLocation,
          "kafka.bootstrap.servers" -> jobConfig.kafkaBootstrapServers,
          "topic" -> jobConfig.kafkaOutputTopic
        )).format("kafka").start()

        writerQuery.awaitTermination()
      }
    }
  }
}

case class SessionsGenerationJobArguments(kafkaBootstrapServers: String = null,
                                          kafkaInputTopic: String = null,
                                          kafkaOutputTopic: String = null,
                                          devicesTableLocation: String = null,
                                          checkpointLocation: String = null) {

  lazy val devicesDeltaTableLocation = s"${devicesTableLocation}/delta"

}
object SessionsGenerationJobArguments {
  val Parser = {
    val builder = OParser.builder[SessionsGenerationJobArguments]
    import builder._
    OParser.sequence(
      opt[String]("kafkaBootstrapServers")
        .action((kafkaBootstrapServers, c) => c.copy(kafkaBootstrapServers = kafkaBootstrapServers)),
      opt[String]("kafkaInputTopic")
        .action((kafkaInputTopic, c) => c.copy(kafkaInputTopic = kafkaInputTopic)),
      opt[String]("kafkaOutputTopic")
        .action((kafkaOutputTopic, c) => c.copy(kafkaOutputTopic = kafkaOutputTopic)),
      opt[String]("devicesTableLocation")
        .action((devicesTableLocation, c) => c.copy(devicesTableLocation = devicesTableLocation)),
      opt[String]("checkpointLocation")
        .action((checkpointLocation, c) => c.copy(checkpointLocation = checkpointLocation)),
    )
  }
}
