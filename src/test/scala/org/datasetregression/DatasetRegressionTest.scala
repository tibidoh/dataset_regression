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

    val regression = DatasetRegression[TestRecord](
      reference,
      test,
      extractKey = r => Map("k" -> r.key),
      extractFields = r => Map("f" -> r.value),
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

    val regression = DatasetRegression[TestRecord](
      reference,
      test,
      extractKey = r => Map("k" -> r.key),
      extractFields = r => Map("f" -> r.value),
      counters = noCounters
    )

    val diff = regression.diff.collect()

    diff should contain(DuplicateTestSample(Map("k" -> "t")))
    diff should contain(DuplicateTestSample(Map("k" -> "rt")))
    diff should contain(DuplicateReferenceSample(Map("k" -> "r")))
    diff should contain(DuplicateReferenceSample(Map("k" -> "rt")))
  }

  it should "allow to override equality condition by type" in {
    val reference = sc.makeRDD(Seq(
      TestRecord("k", Some(1.5))
    ))

    val test = sc.makeRDD(Seq(
      TestRecord("k", Some(1.4))
    ))

    val regression = DatasetRegression[TestRecord](
      reference,
      test,
      extractKey = r => Map("k" -> r.key),
      extractFields = r => Map("f" -> r.value),
      counters = noCounters,
      equalityOverride = {
        case (_, t: Double, r: Double) => (t - r).abs < 0.5 // equal +- 0.5
      }
    )

    val diff = regression.diff.collect()
    diff shouldBe empty
  }

  case class Sample(key: String, regularField: Double, modifiedField: Double)

  it should "allow to override equality condition by field" in {

    //Test sample one has difference in double representation of the second field. This must be handled by equality override
    //Test sample two has same difference in the third field. This should produce a diff

    val reference = sc.makeRDD(Seq(
      ("1", "0.1", "0.1"),
      ("2", "0.1", "0.1")
    ))

    val test = sc.makeRDD(Seq(
      ("1", "1E-1", "0.1"),
      ("2", "0.1", "1E-1")
    ))

    val regression = DatasetRegression[(String, String, String)](
      reference,
      test,
      extractKey = r => Map("key" -> r._1),
      extractFields = r => Map("two" -> Some(r._2), "three" -> Some(r._3)),
      counters = noCounters,
      equalityOverride = {
        case ("two", t: String, r: String) => t.toDouble == r.toDouble
      }
    )

    val diff = regression.diff.collect()
    diff should matchPattern { case Array(DiscrepantSample(_, DiscrepantField("three", "0.1", "1E-1") :: Nil)) => }
  }
}