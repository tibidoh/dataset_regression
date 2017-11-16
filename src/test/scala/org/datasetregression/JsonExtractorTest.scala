package org.datasetregression

import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.scalatest.{FlatSpec, Matchers}

import JsonExtractor._

class JsonExtractorTest extends FlatSpec with Matchers with PerTestSparkSession {
  "ExtractJsonKey" should "support basic xpath-like expressions" in {
    val json = "a" -> ("b" -> "c")
    val extractor = extractJsonKey(Map("result" -> "a/b"))
    extractor(json)("result") shouldEqual "c"
  }

  it should "fail if value is missing" in {
    val json = JObject()
    intercept[IllegalArgumentException] {
      extractJsonKey(Map("" -> "whatever"))(json)
    }
  }

  it should "extract Some if element is found and None if not" in {
    val json = "a" -> 1
    val values = extractJsonFields(Map("yes" -> "a", "no" -> "b"))(json)
    values("yes") shouldEqual Some(1)
    values("no") shouldEqual None
  }

  it should "extract None if element is an object" in {
    val json = "a" -> ("b" -> "c")
    val values = extractJsonFields(Map("r" -> "a"))(json)
    values("r") shouldEqual None
  }

  "ExtractAllJsonFields" should "extract all fields with correct paths" in {
    val json = ("obj" -> ("a" -> "A") ~ ("b" -> "B")) ~ ("arr" -> Seq("ZERO", "ONE")) ~ ("c" -> "C")
    val values = extractAllJsonFields(json)

    values should contain only(
      "obj.a" -> Some("A"),
      "obj.b" -> Some("B"),
      "arr.0" -> Some("ZERO"),
      "arr.1" -> Some("ONE"),
      "c" -> Some("C")
    )
  }
}
