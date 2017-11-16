package org.datasetregression

import org.datasetregression.DatasetRegression.{ComparableFields, PrimaryKey}
import org.json4s.JsonAST._

import scala.collection.Map

object JsonExtractor {
  def extractJsonKey(paths: Map[String, String]): (JObject) => PrimaryKey = {
    extractJsonFields(paths).andThen(a => a.map {
      case (k, Some(v)) => (k, v)
      case (k, None) => throw new IllegalArgumentException(s"Primary key can not have missing fields but $k is missing")
    })
  }

  def extractJsonFields(paths: Map[String, String]): (JObject) => ComparableFields = {
    (json: JObject) => {
      paths.mapValues(path => {
        val pathSegments = path.split('/')
        val firstChild = json \ pathSegments.head
        pathSegments.tail.foldLeft(firstChild)(_ \ _)
      })
        .mapValues {extractPrimitiveValue.orElse { case _ => None }}
    }
  }

  val extractAllJsonFields: (JValue) => ComparableFields =
    doExtractAllJsonFields(_).map { case (k, v) => (k.reverse.mkString("."), v) }


  private def doExtractAllJsonFields(jValue: JValue, path: List[String] = List[String]()): Map[List[String], Option[Any]] =
    jValue match {
      case _ if extractPrimitiveValue.isDefinedAt(jValue) => Map(path -> extractPrimitiveValue(jValue))
      case JObject(fields) => fields.flatMap { case JField(name, value) => doExtractAllJsonFields(value, name :: path) }.toMap
      case JArray(arr) => arr.zip(Stream from 0).flatMap { case (value, index) => doExtractAllJsonFields(value, index.toString :: path) }.toMap
      case unknown => throw new IllegalArgumentException(s"Unknown class of node: ${unknown.getClass}")
    }


  private val extractPrimitiveValue: PartialFunction[JValue, Option[Any]] = {
    case JNothing => None
    case JNull => None
    case JString(value) => Some(value)
    case JDouble(value) => Some(value)
    case JDecimal(value) => Some(value.toDouble)
    case JInt(value) => Some(value.toLong)
    case JBool(value) => Some(value)
  }
}
