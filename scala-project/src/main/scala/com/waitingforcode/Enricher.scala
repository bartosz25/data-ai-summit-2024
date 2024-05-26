package com.waitingforcode

import org.apache.spark.sql.{DataFrame, Dataset, functions => F}

object Enricher {

  def enrichSessionsWithDevices(sessions: Dataset[Session], devices: DataFrame): DataFrame = {
    val enrichedVisits = sessions.join(
      devices,
      sessions("device_type") === F.col("type") &&
      sessions("device_version") === F.col("version"),
      "left_outer"
    ).drop("type", "version", "device_full_name")
      .withColumnRenamed("full_name", "device_full_name")

    enrichedVisits
  }

}
