package com.waitingforcode

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, functions => F}

object Reader {

  def selectRawVisits(visitsSource: DataFrame): DataFrame = {
    val visitSchema: StructType =
      StructType.fromDDL("""
        |visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
        |        context STRUCT<
        |            referral STRING, ad_id STRING,
        |            user STRUCT<
        |                ip STRING, login STRING, connected_since TIMESTAMP
        |            >,
        |            technical STRUCT<
        |                browser STRING, browser_version STRING, network_type STRING, device_type STRING, device_version STRING
        |            >
        |        >
        |""".stripMargin)

    visitsSource.select(F.from_json(
        F.col("value").cast("string"), visitSchema,
      Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ssXXX")).as("value")
    ).selectExpr("value.*")
  }

}
