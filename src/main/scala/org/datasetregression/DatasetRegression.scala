package org.datasetregression

import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.reflect.ClassTag


object DatasetRegression {

  sealed trait FieldDiff
  case class MissingReferenceField(fieldName: String) extends FieldDiff
  case class MissingTestField(fieldName: String) extends FieldDiff
  case class DiscrepantField[T](fieldName: String, referenceValue: T, testValue: T) extends FieldDiff

  type PrimaryKey = Map[String, Any]

  sealed trait SampleDiff {def key: PrimaryKey}
  case class DuplicateReferenceSample(key: PrimaryKey) extends SampleDiff
  case class DuplicateTestSample(key: PrimaryKey) extends SampleDiff
  case class MissingReferenceSample(key: PrimaryKey) extends SampleDiff
  case class MissingTestSample(key: PrimaryKey) extends SampleDiff
  case class DiscrepantSample(key: PrimaryKey, fieldDiffs: List[FieldDiff]) extends SampleDiff

  case class RegressionResult(diff: RDD[SampleDiff], counters: Counters)

  private type FieldsEqual = (String, Any, Any) => Boolean

  type ComparableFields = Map[String, Option[Any]]
  type Counters = Map[String, Long]

  def apply[T: ClassTag](
    reference: RDD[T],
    test: RDD[T],
    extractKey: T => PrimaryKey,
    extractFields: T => ComparableFields,
    counters: (RDD[T], RDD[T], RDD[SampleDiff]) => Counters,
    equalityOverride: PartialFunction[(String, Any, Any), Boolean] = Map.empty
  ): RegressionResult = {


    //Use overrides to compare fields, otherwise use java equality. Not using composition as result of it is not serializable
    val fieldsEqual: FieldsEqual = (k, r, t) => {
      val tuple = (k, r, t)
      if (equalityOverride.isDefinedAt(tuple)) equalityOverride(tuple) else r == t
    }

    val diffs = computeDiffs(reference, test, extractKey, extractFields, fieldsEqual)
    val cntrs = counters(reference, test, diffs)

    RegressionResult(diffs, cntrs)
  }

  private def computeDiffs[T: ClassTag](
    reference: RDD[T], test: RDD[T],
    extractKey: T => PrimaryKey,
    extractFields: T => ComparableFields,
    fieldsEqual: FieldsEqual
  ): RDD[SampleDiff] = {

    (reference.keyBy(extractKey) cogroup test.keyBy(extractKey))
      .flatMap {
        case (key, (Seq(), Seq())) => None
        case (key, (ref, Seq())) =>
          val (r :: rs) = ref.toList //Can't pattern match streams directly
          List(MissingTestSample(key)) ++ rs.map(_ => DuplicateReferenceSample(key))
        case (key, (Seq(), tst)) =>
          val (t :: ts) = tst.toList //Can't pattern match streams directly
          List(MissingReferenceSample(key)) ++ ts.map(_ => DuplicateTestSample(key))
        case (key, (ref, tst)) => {
          //Can't pattern match streams directly
          val (r :: rs) = ref.toList
          val (t :: ts) = tst.toList

          val dups = rs.map(_ => DuplicateReferenceSample(key)) ++ ts.map(_ => DuplicateTestSample(key))
          val diffs = compareSamples(r, t, extractFields, fieldsEqual) match {
            case Nil => List()
            case ds => List(DiscrepantSample(key, ds))
          }

          dups ++ diffs
        }
      }
  }


  private def compareSamples[T](reference: T, test: T, extractFields: T => ComparableFields, fieldsEqual: FieldsEqual): List[FieldDiff] = {
    val baseValues = extractFields(reference)
    val testValues = extractFields(test)

    (baseValues.keys ++ testValues.keys)
      .flatMap(k =>
        (baseValues.get(k).flatten, testValues.get(k).flatten) match {
          case (Some(_), None) => Some(MissingTestField(k))
          case (None, Some(_)) => Some(MissingReferenceField(k))
          case (Some(r), Some(t)) if !fieldsEqual(k, r, t) => Some(DiscrepantField(k, r, t))
          case _ => None
        }
      )
      .toList
  }
}
