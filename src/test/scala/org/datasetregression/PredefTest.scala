package org.datasetregression

import org.datasetregression.Predef._
import org.scalatest.{FlatSpec, Matchers}

class PredefTest extends FlatSpec with Matchers with PerTestSparkSession {
  "StandardCounters" should "be computed correctly" in {
    val reference = sc.makeRDD(Seq(
      TestRecord("diffInField", Some(1))
    ))
    val test = sc.makeRDD(Seq(
      TestRecord("diffInField", Some(2))
    ))

    val regression = DatasetRegression[TestRecord](
      reference,
      test,
      extractKey = r => Map("k" -> r.key),
      extractFields = r => Map("f" -> r.value),
      counters = standardCounters("some-tag")
    )

    regression.counters should have size 7
    regression.counters("some-tag-diffs") shouldBe 1
    regression.counters("some-tag-test-missing") shouldBe 0
  }

  "No counters" should "generate empty set of counters" in {
    val reference = sc.makeRDD(Seq(
      TestRecord("diffInField", Some(1))
    ))
    val test = sc.makeRDD(Seq(
      TestRecord("diffInField", Some(2))
    ))

    val regression = DatasetRegression[TestRecord](
      reference,
      test,
      extractKey = r => Map("k" -> r.key),
      extractFields = r => Map("f" -> r.value),
      counters = noCounters
    )

    regression.counters shouldBe empty
  }
}
