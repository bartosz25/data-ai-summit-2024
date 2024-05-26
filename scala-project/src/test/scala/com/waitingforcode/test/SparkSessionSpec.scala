package com.waitingforcode.test

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

trait SparkSessionSpec extends ExecutionContext {

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .withExtensions(new DeltaSparkSessionExtension())
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.ui.enabled", false)
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

}
