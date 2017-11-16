package org.datasetregression

import org.datasetregression.DatasetRegression._
import org.datasetregression.Predef._
import org.scalatest.{FlatSpec, Matchers}

class DatasetRegressionTest extends FlatSpec with Matchers with PerTestSparkSession {
  it should "generate diff for known cases" in {
    val reference = sc.makeRDD(Seq(
      TestRecord("diffInField", Some(1)),
      TestRecord("missingTestField", Some(1)),
      TestRecord("missingReferenceField", None),
      TestRecord("missingTestRecord", None),
      TestRecord("match", Some(100))
    ))
    val test = sc.makeRDD(Seq(
      TestRecord("diffInField", Some(2)),
      TestRecord("missingTestField", None),
      TestRecord("missingReferenceField", Some(1)),
      TestRecord("missingReferenceRecord", None),
      TestRecord("match", Some(100))
    ))

    val regression = DatasetRegression(
      reference,
      test,
      extractKey = (r: TestRecord) => Map("k" -> r.key),
      extractFields = (r: TestRecord) => Map("f" -> r.value),
      counters = noCounters
    )


    val diff = regression.diff.collect()
    diff should have size 5

    val diffsByCase = diff.map(d => d.key("k") -> d).toMap
    diffsByCase("diffInField") should matchPattern { case DiscrepantSample(_, DiscrepantField("f", 1, 2) :: Nil) => }
    diffsByCase("missingTestField") should matchPattern { case DiscrepantSample(_, MissingTestField("f") :: Nil) => }
    diffsByCase("missingReferenceField") should matchPattern { case DiscrepantSample(_, MissingReferenceField("f") :: Nil) => }
    diffsByCase("missingTestRecord") should matchPattern { case MissingTestSample(_) => }
    diffsByCase("missingReferenceRecord") should matchPattern { case MissingReferenceSample(_) => }
  }

  it should "generate duplicate warnings if keys are not unique" in {
    val reference = sc.makeRDD(Seq(
      TestRecord("r", Some(1)),
      TestRecord("r", Some(1)),
      TestRecord("rt", Some(2)),
      TestRecord("rt", Some(2))
    ))

    val test = sc.makeRDD(Seq(
      TestRecord("t", Some(3)),
      TestRecord("t", Some(3)),
      TestRecord("rt", Some(2)),
      TestRecord("rt", Some(2))
    ))

    val regression = DatasetRegression(
      reference,
      test,
      extractKey = (r: TestRecord) => Map("k" -> r.key),
      extractFields = (r: TestRecord) => Map("f" -> r.value),
      counters = noCounters
    )

    val diff = regression.diff.collect()

    diff should contain(DuplicateTestSample(Map("k" -> "t")))
    diff should contain(DuplicateTestSample(Map("k" -> "rt")))
    diff should contain(DuplicateReferenceSample(Map("k" -> "r")))
    diff should contain(DuplicateReferenceSample(Map("k" -> "rt")))
  }
}