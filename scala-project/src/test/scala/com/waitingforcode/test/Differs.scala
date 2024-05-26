package com.waitingforcode.test

import com.waitingforcode.builders.DeviceUnderTest
import com.waitingforcode._
import difflicious.Differ

import java.sql.Timestamp

object Differs {

  implicit val TimestampDiffer: Differ[Timestamp] = {
    Differ.useEquals[Timestamp](ts => {
      if (ts == null) "" else ts.toString
    })
  }
  val DeviceDiffer: Differ[DeviceUnderTest] = Differ.derived[DeviceUnderTest]
  implicit val SessionPageDiffer: Differ[SessionPage] = Differ.derived[SessionPage]
  val SessionDiffer: Differ[Session] = Differ.derived[Session]
  val SessionStateDiffer: Differ[SessionState] = Differ.derived[SessionState]

  // Note: the declaration order does matter; e.g define the differ for Visit first to see the
  //       assertions using it failing
  implicit val DifferUserContext: Differ[UserContext] = Differ.derived[UserContext]
  implicit val DifferTechnicalContext: Differ[TechnicalContext] = Differ.derived[TechnicalContext]
  implicit val DifferVisitContext: Differ[VisitContext] = Differ.derived[VisitContext]
  implicit val VisitDiffer: Differ[Visit] = Differ.derived[Visit]
  val VisitsListDiffer: Differ[List[Visit]] = Differ.derived[List[Visit]]
}
