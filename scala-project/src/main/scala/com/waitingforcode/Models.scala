package com.waitingforcode

import java.sql.Timestamp

case class TechnicalContext(browser: String, browser_version: String, network_type: String,
                            device_type: String, device_version: String)
case class UserContext(ip: String, login: Option[String], connected_since: Option[Timestamp])
case class VisitContext(referral: String, ad_id: Option[String], user: Option[UserContext],
                        technical: Option[TechnicalContext])
case class Visit(visit_id: String, event_time: Timestamp,
                                user_id: Option[String], page: String, context: Option[VisitContext] = None) {

  def getDeviceTypeIfDefined: Option[String] = {
    context.flatMap(c => c.technical.map(t => t.device_type))
  }

  def getDeviceVersionIfDefined: Option[String] = {
    context.flatMap(c => c.technical.map(t => t.device_version))
  }
}


case class SessionPage(page: String, visit_time: Timestamp)
case class Session(visit_id: String, user_id: Option[String], start_time: Timestamp,
                   end_time: Timestamp, visited_pages: Seq[SessionPage],
                   device_type: Option[String], device_version: Option[String],
                   device_full_name: Option[String])