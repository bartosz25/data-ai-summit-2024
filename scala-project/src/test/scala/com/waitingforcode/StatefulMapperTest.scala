package com.waitingforcode

import com.waitingforcode.test.Differs.SessionStateDiffer
import com.waitingforcode.builders.SessionUnderTest.{page, sessionState, sessionStateToGroupState}
import com.waitingforcode.builders.VisitUnderTest.build
import com.waitingforcode.builders.stringToTimestamp
import com.waitingforcode.test.{Differs, ExecutionContext}
import difflicious.scalatest.ScalatestDiff.DifferExtensions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class StatefulMapperTest extends AnyFlatSpec with Matchers with ExecutionContext {

  behavior of "stateful mapper"

  it should "emit a completed session" in {
    val state = sessionState()(page("page 1", "2024-05-01T10:00:00Z"),
      page("page 2", "2024-05-01T10:03:00Z"), page("page 3", "2024-05-01T10:05:00Z"))
    val currentState = sessionStateToGroupState(state, timedOut = true)

    val mappedSession = StatefulMapper.mapVisitsToSessions("visit 1", Iterator.empty, currentState).toSeq

    mappedSession should have size 1
    Differs.SessionDiffer.assertNoDiff(
      mappedSession.head,
      Session(
        visit_id = "visit 1", user_id = state.user_id, start_time = "2024-05-01T10:00:00Z",
        end_time = "2024-05-01T10:05:00Z", visited_pages = state.visits,
        device_type = state.device_type, device_version = state.device_version,
        device_full_name = None
      )
    )
  }

  it should "create a state when the watermark is empty (0)" in {
    val visit1_1 = build(visit_id="v1", event_time="2024-01-05T10:00:00.000Z", page="page 1")
    val visit1_2 = build(visit_id="v1", event_time="2024-01-05T10:04:00.000Z", page="page 2")
    val currentState = sessionStateToGroupState(null, timedOut = false, watermark = null)

    val mappedSession = StatefulMapper.mapVisitsToSessions("visit 1", Iterator(visit1_1, visit1_2), currentState).toSeq

    mappedSession shouldBe empty
    currentState.getOption shouldBe defined
    SessionStateDiffer.assertNoDiff(
      currentState.get,
      SessionState(
        user_id = visit1_1.user_id, visits = Seq(
          page("page 1", "2024-01-05T10:00:00.000Z"), page("page 2", "2024-01-05T10:04:00.000Z")
        ),
        device_type = visit1_1.getDeviceTypeIfDefined, device_version = visit1_1.getDeviceVersionIfDefined
      )
    )
  }

  it should "create a state when the watermark is set to 2024-01-05 09:50 AM" in {
    val visit1_1 = build(visit_id="v1", event_time="2024-01-05T10:00:00.000Z", page="page 1")
    val visit1_2 = build(visit_id="v1", event_time="2024-01-05T10:04:00.000Z", page="page 2")
    val currentState = sessionStateToGroupState(null, timedOut = false, watermark = "2024-01-05T09:05:00.000Z".getTime)

    val mappedSession = StatefulMapper.mapVisitsToSessions("visit 1", Iterator(visit1_1, visit1_2), currentState).toSeq

    mappedSession shouldBe empty
    currentState.getOption shouldBe defined
    SessionStateDiffer.assertNoDiff(
      currentState.get,
      SessionState(
        user_id = visit1_1.user_id, visits = Seq(
          page("page 1", "2024-01-05T10:00:00.000Z"), page("page 2", "2024-01-05T10:04:00.000Z")
        ),
        device_type = visit1_1.getDeviceTypeIfDefined, device_version = visit1_1.getDeviceVersionIfDefined
      )
    )
  }


  it should "create a state and restore it when the watermark is set to 2024-01-05 09:50 AM" in {
    val visit1_1 = build(visit_id="v1", event_time="2024-01-05T10:00:00.000Z", page="page 1")
    val visit1_2 = build(visit_id="v1", event_time="2024-01-05T10:04:00.000Z", page="page 2")
    val currentState = sessionStateToGroupState(null, timedOut = false, watermark = "2024-01-05T09:05:00.000Z".getTime)

    val mappedSession = StatefulMapper.mapVisitsToSessions("visit 1", Iterator(visit1_1, visit1_2), currentState).toSeq

    mappedSession shouldBe empty
    currentState.getOption shouldBe defined
    SessionStateDiffer.assertNoDiff(
      currentState.get,
      SessionState(
        user_id = visit1_1.user_id, visits = Seq(
          page("page 1", "2024-01-05T10:00:00.000Z"), page("page 2", "2024-01-05T10:04:00.000Z")
        ),
        device_type = visit1_1.getDeviceTypeIfDefined, device_version = visit1_1.getDeviceVersionIfDefined
      )
    )

    val newState = sessionStateToGroupState(currentState.get, timedOut = false, watermark = "2024-01-05T10:03:00.000Z".getTime)
    val visit1_3 = build(visit_id="v1", event_time="2024-01-05T10:06:00.000Z", page="page 3")
    val visit1_4 = build(visit_id="v1", event_time="2024-01-05T10:07:00.000Z", page="page 4")
    val visit1_5 = build(visit_id="v1", event_time="2024-01-05T10:08:00.000Z", page="page 5")

    val mappedSessionUpdated = StatefulMapper.mapVisitsToSessions("visit 1", Iterator(visit1_3, visit1_4, visit1_5),
      newState).toSeq

    mappedSessionUpdated shouldBe empty
    newState.getOption shouldBe defined
    SessionStateDiffer.assertNoDiff(
      newState.get,
      SessionState(
        user_id = visit1_1.user_id, visits = Seq(
          page("page 1", "2024-01-05T10:00:00.000Z"), page("page 2", "2024-01-05T10:04:00.000Z"),
          page("page 3", "2024-01-05T10:06:00.000Z"), page("page 4", "2024-01-05T10:07:00.000Z"),
          page("page 5", "2024-01-05T10:08:00.000Z")
        ),
        device_type = visit1_1.getDeviceTypeIfDefined, device_version = visit1_1.getDeviceVersionIfDefined
      )
    )
  }

}
