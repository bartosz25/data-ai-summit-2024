package com.waitingforcode

import com.waitingforcode.SessionsGenerationJobLogic.generateSessions
import com.waitingforcode.builders.DeviceUnderTest.commonDevices
import com.waitingforcode.builders.SessionUnderTest.fromJsonToSession
import com.waitingforcode.builders.VisitUnderTest.visitToJsonString
import com.waitingforcode.builders.{VisitUnderTest, stringToTimestamp}
import com.waitingforcode.dataset.{DataToAssertWriterReader, DatasetWriter}
import com.waitingforcode.test.{Differs, SparkSessionSpec}
import difflicious.scalatest.ScalatestDiff.DifferExtensions
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SessionsGenerationJobLogicWhenWatermarkPassesByTest extends AnyFlatSpec with Matchers with SparkSessionSpec {

  "the job" should "generate a session when the watermark passes by" in {
    val Seq(device1, device2) = commonDevices()
    import sparkSession.implicits._
    val devicesToTest = Seq(device1, device2).toDF
    val testName = "watermark_passes_by"
    val datasetWriter = new DatasetWriter(s"/tmp/visits_logic_${testName}")
    val rawVisits_1 = Seq(
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:00:00.000Z", page="page 2")
    )
    datasetWriter.writeDataFrame(rawVisits_1.map(visit => visitToJsonString(visit)).toDF("value"))
    val dataToAssertManager = new DataToAssertWriterReader(sparkSession)

    val visitsReader = sparkSession.readStream.schema("value STRING").json(datasetWriter.outputDir)
    val sessionsWriter = generateSessions(visitsReader, devicesToTest, Trigger.ProcessingTime(0L),
      datasetWriter.checkpointDir)
    val startedQuery = sessionsWriter.foreachBatch(dataToAssertManager.writeMicroBatch _).start()

    startedQuery.processAllAvailable()
    val emittedSessions_0 = dataToAssertManager.getResultsForMicroBatch(0)
    emittedSessions_0 shouldBe empty
    val emittedSessions_1 = dataToAssertManager.getResultsForMicroBatch(1)
    emittedSessions_1 shouldBe empty

    val rawVisits_2 = Seq(
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T09:59:00.000Z", page="page 3"),
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:03:00.000Z", page="page 4"),
      VisitUnderTest.build(visit_id="v2", event_time="2024-01-05T10:00:00.000Z", user_id=None, page="home_page")
    )
    datasetWriter.writeDataFrame(rawVisits_2.map(visit => visitToJsonString(visit)).toDF("value"))

    startedQuery.processAllAvailable()
    val emittedSessions_2 = dataToAssertManager.getResultsForMicroBatch(2)
    emittedSessions_2 shouldBe empty
    val emittedSessions_3 = dataToAssertManager.getResultsForMicroBatch(3)
    emittedSessions_3 shouldBe empty

    val rawVisits_3 = Seq(
      VisitUnderTest.build(visit_id="v3", event_time="2024-01-05T10:13:00.000Z", page="page 5")
    )
    datasetWriter.writeDataFrame(rawVisits_3.map(visit => visitToJsonString(visit)).toDF("value"))

    startedQuery.processAllAvailable()
    val emittedSessions_4 = dataToAssertManager.getResultsForMicroBatch(4)
    emittedSessions_4 shouldBe empty
    val emittedSessions_5 = dataToAssertManager.getResultsForTheLastMicroBatch
    emittedSessions_5 should have size 2
    val sessionsToAssert = fromJsonToSession(emittedSessions_5).groupBy(session => session.visit_id)
    Differs.SessionDiffer.assertNoDiff(
      sessionsToAssert("v1").head,
      Session(visit_id = "v1", user_id = Some("user A id"),
        start_time = "2024-01-05T09:59:00.000Z", end_time = "2024-01-05T10:03:00.000Z",
        visited_pages = Seq(SessionPage("page 3", "2024-01-05T09:59:00.000Z"),
          SessionPage("page 2", "2024-01-05T10:00:00.000Z"), SessionPage("page 4", "2024-01-05T10:03:00.000Z")),
        device_type = Some("pc"), device_version = Some("1.2.3"), device_full_name = Some(device1.full_name)
    ))
    Differs.SessionDiffer.assertNoDiff(
      sessionsToAssert("v2").head,
      Session(visit_id = "v2", user_id = None,
        start_time = "2024-01-05T10:00:00.000Z", end_time = "2024-01-05T10:00:00.000Z",
        visited_pages = Seq(SessionPage("home_page", "2024-01-05T10:00:00.000Z")),
        device_type = Some("pc"), device_version = Some("1.2.3"),  device_full_name = Some(device1.full_name))
    )
  }

}
