package com.waitingforcode

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.streaming.GroupState

import java.util.concurrent.TimeUnit
import scala.math.max

object StatefulMapper {

  private val MapperLogger: Logger = LogManager.getLogger(classOf[StatefulMapper.type])
  private val Time10MinutesAsMillis = TimeUnit.MINUTES.toMillis(10)

  def mapVisitsToSessions(visitId: String, visits: Iterator[Visit],
                          groupState: GroupState[SessionState]): Iterator[Session] = {
    if (groupState.hasTimedOut) {
      val outputSession = generateOutputForExpiredState(visitId, groupState.get)
      Iterator(outputSession)
    } else {
      val currentPages = groupState.getOption.map(state => state.visits).getOrElse(Seq.empty)

      var baseEventTimeForTimeout: Long = groupState.getCurrentWatermarkMs()
      var deviceType: Option[String] = None
      var deviceVersion: Option[String] = None
      var userId: Option[String] = None
      val mappedVisits = visits.map(visit => {
        // The watermark is missing in the first micro-batch; we're using the max event time for
        // each visit here
        if (groupState.getCurrentWatermarkMs == 0) {
          baseEventTimeForTimeout = max(baseEventTimeForTimeout, visit.event_time.getTime)
        }
        userId = visit.user_id
        if (deviceType.isEmpty && visit.getDeviceTypeIfDefined.isDefined) {
          deviceType = visit.getDeviceTypeIfDefined
        }
        if (deviceVersion.isEmpty && visit.getDeviceVersionIfDefined.isDefined) {
          deviceVersion = visit.getDeviceVersionIfDefined
        }
        SessionPage(visit.page, visit.event_time)
      })

      val newState = SessionState(visits = currentPages ++ mappedVisits,
        device_type = deviceType, device_version = deviceVersion,
        user_id = userId
      )
      groupState.update(newState)

      val newExpirationTime = baseEventTimeForTimeout + Time10MinutesAsMillis
      groupState.setTimeoutTimestamp(newExpirationTime)
      MapperLogger.info(s"Updating ${newState} with expiration time ${newExpirationTime}")
      Iterator.empty
    }
  }

  private def generateOutputForExpiredState(visitId: String, sessionState: SessionState): Session = {
    val sortedPages = sessionState.visits.sortBy(page => page.visit_time)
    val sessionStartTime = sortedPages.head.visit_time
    val sessionEndTime = sortedPages.last.visit_time

    Session(
      visit_id = visitId, user_id = sessionState.user_id, start_time = sessionStartTime,
      end_time = sessionEndTime, visited_pages = sortedPages,
      device_type = sessionState.device_type, device_version = sessionState.device_version,
      device_full_name = None
    )
  }

}

case class SessionState(visits: Seq[SessionPage], device_type: Option[String],
                        device_version: Option[String], user_id: Option[String])