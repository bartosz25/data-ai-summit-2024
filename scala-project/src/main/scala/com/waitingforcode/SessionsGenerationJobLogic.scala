package com.waitingforcode

import org.apache.spark.sql.streaming.{DataStreamWriter, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions => F}

object SessionsGenerationJobLogic {

  def generateSessions(visitsSource: DataFrame, devices: DataFrame,
                        trigger: Trigger, checkpointLocation: String): DataStreamWriter[Row] = {
    import visitsSource.sparkSession.implicits._

    val rawVisits = Reader.selectRawVisits(visitsSource).as[Visit]

    val groupedVisits = rawVisits.withWatermark("event_time", "5 minutes")
      .groupByKey(row => row.visit_id)

    val sessions: Dataset[Session] = groupedVisits.flatMapGroupsWithState(
      timeoutConf = GroupStateTimeout.EventTimeTimeout(),
      outputMode = OutputMode.Append()
    )(StatefulMapper.mapVisitsToSessions)

    val enrichedSessions = Enricher.enrichSessionsWithDevices(sessions, devices)

    val sessionsToWrite = enrichedSessions
      .withColumn("value", F.to_json(F.struct("*")))
      .withColumn("key", F.col("visit_id"))
      .select("key", "value")

    Writer.setUpSessionsWriter(sessionsToWrite, trigger).option("checkpointLocation", checkpointLocation)
  }

}
