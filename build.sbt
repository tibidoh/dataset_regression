name := "DatasetRegression"
version := "0.1"
scalaVersion := "2.11.11"

// Quick and dirty way to have one spark session at a time
parallelExecution in Test := false

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.5.3"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"