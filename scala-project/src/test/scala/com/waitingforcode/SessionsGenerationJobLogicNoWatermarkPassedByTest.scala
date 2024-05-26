package com.waitingforcode

import com.waitingforcode.SessionsGenerationJobLogic.generateSessions
import com.waitingforcode.builders.DeviceUnderTest.commonDevices
import com.waitingforcode.builders.VisitUnderTest
import com.waitingforcode.builders.VisitUnderTest.visitToJsonString
import com.waitingforcode.dataset.{DataToAssertWriterReader, DatasetWriter}
import com.waitingforcode.test.SparkSessionSpec
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SessionsGenerationJobLogicNoWatermarkPassedByTest extends AnyFlatSpec with Matchers with SparkSessionSpec {

  "the job" should "not generate a session as the watermark didn't pass by" in {
    val Seq(device1, device2) = commonDevices()
    import sparkSession.implicits._
    val devicesToTest = Seq(device1, device2).toDF
    val testName = "watermark_didnt_pass_by"
    val datasetWriter = new DatasetWriter(s"/tmp/visits_logic_${testName}")
    val rawVisits_1 = Seq(
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:00:00.000Z", page="page 2"),
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:01:00.000Z", page="page 3"),
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:03:00.000Z", page="page 4"),
      VisitUnderTest.build(visit_id="v2", event_time="2024-01-05T10:00:00.000Z", user_id=None, page="home_page")
    )
    datasetWriter.writeDataFrame(rawVisits_1.map(visit => visitToJsonString(visit)).toDF("value"))
    val dataToAssertManager = new DataToAssertWriterReader(sparkSession)

    val visitsReader = sparkSession.readStream.schema("value STRING").json(datasetWriter.outputDir)
    val sessionsWriter = generateSessions(visitsReader, devicesToTest, Trigger.ProcessingTime(0L))
    val startedQuery = sessionsWriter.foreachBatch(dataToAssertManager.writeMicroBatch _).start()

    startedQuery.processAllAvailable()
    val emittedSessions_0 = dataToAssertManager.getResultsForMicroBatch(0)
    emittedSessions_0 shouldBe empty
    val emittedSessions_1 = dataToAssertManager.getResultsForMicroBatch(1)
    emittedSessions_1 shouldBe empty

    val rawVisits_2 = Seq(
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:04:00.000Z", page="page 5"),
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:05:00.000Z", page="page 6"),
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:07:00.000Z", page="page 7"),
    )
    datasetWriter.writeDataFrame(rawVisits_2.map(visit => visitToJsonString(visit)).toDF("value"))

    startedQuery.processAllAvailable()
    val emittedSessions_2 = dataToAssertManager.getResultsForMicroBatch(2)
    emittedSessions_2 shouldBe empty
    val emittedSessions_3 = dataToAssertManager.getResultsForMicroBatch(3)
    emittedSessions_3 shouldBe empty
  }

}
