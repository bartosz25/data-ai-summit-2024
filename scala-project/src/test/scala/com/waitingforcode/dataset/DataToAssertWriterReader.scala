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

  /**
   * @return Results for the last micro-batch. We're relying on it because sometimes Spark executed two queries
   *         instead of one, where only the last one does the cleaning. Hence, instead of getting
   *         two queries after each processAllAvailable call, we get three.
   *         It looks like a weird race condition between the processAllAvailable() and the state cleaning process.
   *
   *         As a result, using the direct number-based fetch, leads to flaky tests.
   */
  def getResultsForTheLastMicroBatch: Seq[String] = resultsPerMicroBatch.last._2

}
