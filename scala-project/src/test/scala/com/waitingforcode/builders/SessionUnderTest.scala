package com.waitingforcode.builders

import com.waitingforcode.{Session, SessionPage, SessionState}
import org.apache.spark.api.java.Optional
import org.apache.spark.sql.catalyst.plans.logical.EventTimeTimeout
import org.apache.spark.sql.streaming.{GroupState, TestGroupState}

object SessionUnderTest {

  def page(page: String, visitTime: String): SessionPage = {
    SessionPage(
      page = page, visit_time = stringToTimestamp(visitTime)
    )
  }

  def session(visitId: String = "visit_1", userId: String = "user A", deviceFullName: String = "pc v1",
              deviceType: String = "pc", deviceVersion: String = "v1")(pages: SessionPage*): Session = {
    val sortedPages = pages.sortBy(p => p.visit_time)

    Session(
      visit_id = visitId, user_id = Option(userId),
      start_time = sortedPages.head.visit_time,
      end_time = sortedPages.last.visit_time,
      visited_pages = sortedPages,
      device_type = Option(deviceType), device_version = Option(deviceVersion),
      device_full_name = Option(deviceFullName)
    )
  }

  def sessionState(deviceType: String = "mac", deviceVersion: String = "version 1",
                   userId: String = "user A")(pages: SessionPage*): SessionState = {
    SessionState(
      visits = pages, device_type = Option(deviceType), device_version = Option(deviceVersion),
      user_id = Option(userId)
    )
  }

  def sessionStateToGroupState(sessionState: SessionState,
                               watermark: java.lang.Long = 0L,
                               timedOut: Boolean = false): GroupState[SessionState] = {
    TestGroupState.create[SessionState](
      optionalState = Optional.ofNullable(sessionState),
      hasTimedOut = timedOut,
      eventTimeWatermarkMs = Optional.ofNullable(watermark),
      batchProcessingTimeMs = System.currentTimeMillis(),
      timeoutConf = EventTimeTimeout
    )
  }

  def fromJsonToSession(jsonLines: Seq[String]): Seq[Session] = {
    jsonLines.map(line => {
      JsonMapperForSessions.readValue(line, classOf[Session])
    })
  }

}
