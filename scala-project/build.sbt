name := "data-ai-summit-2024-unit-tests-structured-streaming-demo"

version := "1.0"

scalaVersion := "2.13.13"

val sparkVersion = "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % "test"
libraryDependencies += "com.github.jatcwang" %% "difflicious-scalatest" % "0.4.2"  % "test"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

libraryDependencies += "io.delta" %% "delta-spark" % "3.1.0"
