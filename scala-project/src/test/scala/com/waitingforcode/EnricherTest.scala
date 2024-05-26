package com.waitingforcode

import com.waitingforcode.builders.SessionUnderTest.{page, session}
import com.waitingforcode.builders._
import com.waitingforcode.test.{Differs, SparkSessionSpec}
import difflicious.scalatest.ScalatestDiff._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EnricherTest extends AnyFlatSpec with Matchers with SparkSessionSpec {

  import sparkSession.implicits._
  behavior of "enricher"

  it should "keep the session even if there is no matching device" in {
    val devices = Seq(DeviceUnderTest.build(device_type = "mac")).toDF
    val sessionToTest = session(deviceFullName = null)(page("page1", "2024-05-06T10:00:12Z"))
    val sessions = Seq(sessionToTest).toDS

    val enrichedSessions = Enricher.enrichSessionsWithDevices(sessions, devices).as[Session].collect()

    enrichedSessions should have size 1
    Differs.SessionDiffer.assertNoDiff(
      enrichedSessions(0),
      sessionToTest
    )
  }


  it should "combine session with the matching device" in {
    val deviceToTest = DeviceUnderTest.build(device_type = "mac", version = "ver 1.0.0",
      full_name = "mac v1.0.0 - en")
    val devices = Seq(deviceToTest).toDF
    val sessionToTest = session(
      deviceType = "mac", deviceVersion = "ver 1.0.0", deviceFullName = null
    )(page("page1", "2024-05-06T10:00:12Z"))
    val sessions = Seq(sessionToTest).toDS

    val enrichedSessions = Enricher.enrichSessionsWithDevices(sessions, devices).as[Session].collect()


    enrichedSessions should have size 1
    Differs.SessionDiffer.assertNoDiff(
      enrichedSessions(0),
      sessionToTest.copy(
        device_full_name = Some(deviceToTest.full_name)
      )
    )
  }

}
