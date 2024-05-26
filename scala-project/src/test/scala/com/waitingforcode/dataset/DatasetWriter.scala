package com.waitingforcode.dataset

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

class DatasetWriter(val outputDir: String) {

  FileUtils.deleteDirectory(new File(outputDir))

  def writeDataFrame(dataFrame: DataFrame): Unit = {
    dataFrame.write.mode(SaveMode.Append).json(outputDir)
  }

}
