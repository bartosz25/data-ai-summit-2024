package com.waitingforcode

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}

object Writer {

  def setUpSessionsWriter(sessionsToWrite: DataFrame,
                          trigger: Trigger): DataStreamWriter[Row] = {
    sessionsToWrite.writeStream.outputMode(OutputMode.Append)
      .trigger(trigger)
  }

}
