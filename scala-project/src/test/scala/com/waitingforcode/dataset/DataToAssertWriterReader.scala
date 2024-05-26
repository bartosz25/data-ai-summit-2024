package com.waitingforcode.dataset

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class DataToAssertWriterReader(sparkSession: SparkSession) {

  private val resultsPerMicroBatch = new mutable.HashMap[Long, Seq[String]]

  def writeMicroBatch(dataFrame: DataFrame, microBatch: Long): Unit = {
    import sparkSession.implicits._
    val rows = dataFrame.select("value").as[String].collect()
    resultsPerMicroBatch.put(microBatch, rows)
    ()
  }

  def getResultsForMicroBatch(microBatch: Long): Seq[String] = resultsPerMicroBatch(microBatch)

}
