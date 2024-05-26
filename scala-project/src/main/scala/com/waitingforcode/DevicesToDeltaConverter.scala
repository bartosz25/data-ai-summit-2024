package com.waitingforcode

import org.apache.spark.sql.{SaveMode, SparkSession}

object DevicesToDeltaConverter {

  def convertDevicesToDeltaTable(sparkSession: SparkSession, devicesInput: String,
                                 devicesOutput: String): Unit = {
    // The data generator doesn't support Delta Lake sink yet; converting it quickly before starting the job
    val inputDevices = sparkSession.read.schema("type STRING, full_name STRING, version STRING").json(devicesInput)

    inputDevices.write.mode(SaveMode.Overwrite).format("delta").save(devicesOutput)
  }

}
