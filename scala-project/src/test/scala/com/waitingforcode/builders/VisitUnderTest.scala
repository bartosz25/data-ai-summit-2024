package com.waitingforcode.builders

import com.waitingforcode.{TechnicalContext, UserContext, Visit, VisitContext}

import java.sql.Timestamp

object VisitUnderTest {

  def build(visit_id: String = "visit_1", event_time: String = "2024-01-05T10:00:00.000Z",
            user_id: Option[String] = Some("user A id"), page: String = "page1.html",
            referral: String = "search", ad_id: String = "ad 1",
            user_context: UserContext = UserContextUnderTest.build(),
            technical_context: TechnicalContext = TechnicalContextUnderTest.build()): Visit = {

    Visit(
      visit_id = visit_id, event_time = stringToTimestamp(event_time),
      user_id = user_id, page = page,
      context = Some(VisitContext(
        referral = referral, ad_id = Option(ad_id), user = Option(user_context),
        technical = Option(technical_context)
      ))
    )
  }

  def visitToJsonString(visit: Visit): String = {
    JsonMapper.writeValueAsString(visit)
  }

}
object UserContextUnderTest {

  def build(ip: String = "1.1.1.1", login: String = "user A login",
            connected_since: String = "2024-01-05T11:00:00.000Z"): UserContext = {
    val connectedSinceFinalValue: Option[Timestamp] = nullableStringToOptionalTimestamp(connected_since)
    UserContext(ip = ip, login = Option(login),
      connected_since = connectedSinceFinalValue)
  }
}

object TechnicalContextUnderTest {


  def build(browser: String = "Firefox", browser_version: String = "3.2.1",
            network_type: String = "wifi", device_type: String ="pc",
            device_version: String = "1.2.3"): TechnicalContext = {

    TechnicalContext(browser = browser, browser_version = browser_version,
      network_type = network_type, device_type = device_type, device_version = device_version)
  }
}