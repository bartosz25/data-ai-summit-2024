package com.waitingforcode

import com.waitingforcode.builders.VisitUnderTest
import com.waitingforcode.builders.VisitUnderTest.visitToJsonString
import com.waitingforcode.test.{Differs, SparkSessionSpec}
import difflicious.Differ
import difflicious.scalatest.ScalatestDiff._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReaderTest extends AnyFlatSpec with Matchers with SparkSessionSpec {

  "reader" should "extract JSON to top level DataFrame columns" in {
    val rawVisits = Seq(
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:00:00.000Z", page="page 2"),
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:01:00.000Z", page="page 3"),
      VisitUnderTest.build(visit_id="v1", event_time="2024-01-05T10:03:00.000Z", page="page 4"),
      VisitUnderTest.build(visit_id="v2", event_time="2024-01-05T10:00:00.000Z", user_id=None, page="home_page")
    )
    import sparkSession.implicits._
    val rawVisitsToTest = (rawVisits.map(visit => visitToJsonString(visit)) ++ Seq("{...}")).toDF("value")
    val transformedVisits = Reader.selectRawVisits(rawVisitsToTest)
    val visitsToAssert: Array[Visit] = transformedVisits.as[Visit].collect()
    visitsToAssert should have size (rawVisits.size + 1)
    Differs.VisitsListDiffer.assertNoDiff(
      visitsToAssert.toList,
      rawVisits.toList ++ List(Visit(null, null, None, null, None))
    )
  }

}
