package org.datasetregression

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterEach, Suite}

trait PerTestSparkSession extends BeforeAndAfterEach { self: Suite =>

  private var currentSession: Option[SparkSession] = None

  def ss: SparkSession = getOrCreateSession
  def sc: SparkContext = ss.sparkContext
  def sq: SQLContext = ss.sqlContext

  private def getOrCreateSession = synchronized {
    if (currentSession.isEmpty)
      currentSession = Some(
        SparkSession.builder()
          .master("local[4]")
          .appName("test")
          .config("spark.ui.enabled", value = false)
          .getOrCreate())
    currentSession.get
  }

  override def afterEach() {
    synchronized {
      currentSession.foreach(_.stop())
      currentSession = None
    }
    super.afterEach()
  }
}

