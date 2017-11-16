package org.datasetregression

import java.util.concurrent.Executors

import org.apache.spark.rdd.RDD
import org.datasetregression.DatasetRegression._
import org.json4s.JsonAST._

import scala.collection.Map
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

object Predef {

  def standardCounters[T: ClassTag](tag: String): (RDD[T], RDD[T], RDD[SampleDiff]) => Counters =
    (ref: RDD[T], test: RDD[T], diff: RDD[SampleDiff]) => {

      val executorService = Executors.newFixedThreadPool(3)
      implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

      val refCounters = Future {Map(s"$tag-ref" -> ref.count)}
      val testCounters = Future {Map(s"$tag-test" -> test.count)}

      val counterNames = Map[Class[_ <: SampleDiff], String](
        classOf[MissingReferenceSample] -> s"$tag-ref-missing",
        classOf[MissingTestSample] -> s"$tag-test-missing",
        classOf[DuplicateReferenceSample] -> s"$tag-ref-duplicated",
        classOf[DuplicateTestSample] -> s"$tag-test-duplicated",
        classOf[DiscrepantSample] -> s"$tag-diffs"
      )

      val diffCounters = Future {
        diff
          .map(rowDiff => Map(counterNames(rowDiff.getClass) -> 1L))
          .fold(counterNames.values.map(_ -> 0L).toMap) { case (l, r) =>
          (l.keySet ++ r.keySet)
            .map { k => (k, l.getOrElse(k, 0L) + r.getOrElse(k, 0L)) }
            .toMap
        }
      }

      Await.result(Future.sequence(List(diffCounters, refCounters, testCounters)), Duration.Inf).reduce(_ ++ _)
    }


  def noCounters[T: ClassTag]: (RDD[T], RDD[T], RDD[SampleDiff]) => Counters =
    (ref: RDD[T], test: RDD[T], diff: RDD[SampleDiff]) => Map()
}

