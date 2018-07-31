# Dataset regression tool

Big data applications built with Spark or Hadoop produce datasets as series of files with no particular guarantee of neither distribution of data samples between the files, nor ordering within a file.
Building regression tooling for such application can be a tedious task, but can be greatly simplified with this fixture. It uses Spark to find matching samples
in reference and tested data sets, generates human readable diffs, detects duplicates and automatically generates basic stats of a regression run.

### Configuring and launching

The fixture does not rely on underlying data format. Instead, it uses user defined closures to extract a key and values from a data sample. This allows it to be
agnostic of data format and type, while being able to support data type specific DSLs for data extraction.

A simple call to regression fixture for typed input may look like:
```scala
case class ShuttleLaunch(num: Int, timestamp: LocalDateTime, vessel: String)

val reference: RDD[ShuttleLaunch] = ...
val test: RDD[ShuttleLaunch] = ...

val regression = DatasetRegression[ShuttleLaunch](
    reference,
    test,
    extractKey = l => Map("num" -> l.num),
    extractFields = l => Map("timestamp" -> Some(l.timestamp), "vessel" -> Some(l.vessel)),
    counters = standardCounters
)
```

For a weakly typed input, such as in json lines, a similar call may look like the following:
```scala
import org.json4s.JsonAST._

val reference: RDD[JObject] = ...
val test: RDD[JObject] = ...

import JsonExtractor._

val regression = DatasetRegression[JObject](
    reference,
    test,
    extractKey = extractJsonKey(Map("num" -> "num")),
    extractFields = extractJsonValues(Map("timestamp" -> "timestamp", "vessel" -> "vessel/name")),
    counters = standardCounters
)
```
Note using a DSL for key and value extraction. This could also be done manually like in the first example, though.

In certain cases we might need different equality conditions for some of the fields. For instance, if we want to tolerate different
times of launch but only want to compare dates, it can be done with an override:
```scala
val regression = DatasetRegression[ShuttleLaunch](
    reference,
    test,
    extractKey = l => Map("num" -> l.num),
    extractFields = l => Map("timestamp" -> Some(l.timestamp), "vessel" -> Some(l.vessel)),
    counters = standardCounters,
    equalityOverride = {
        case ("timestamp", t: LocalDateTime, r: LocalDateTime) => t.toLocalDate == r.toLocalDate
    }
)
```

### Results interpretation

Result of regression is returned as `RegressionResult` object containing two fields:

`counters:Map[String, Long]`  - A set of counters created by a function passed to 'counters' parameter.

`diff:RDD[SampleDiff]` - RDD of objects describing diffs found between two input RDDs. Each SampleDiff object has 
a `key` field which (surprise) hold it's key. Several possible cases supported:
 * `DiscrepantSample` - Samples from input datasets successfully matched but one or more field values are different
 * `MissingReferenceSample` - Sample found in test dataset is missing from reference dataset
 * `MissingTestSample` - Sample found in reference dataset is missing in test dataset
 * `DuplicateReferenceSample` - Multiple samples found for the same key in reference dataset (Usually means incorrectly configured key extraction)
 * `DuplicateTestSample` - Multiple samples found for the same key in test dataset (Usually means incorrectly configured key extraction)
 
### Note on counters
The ultimate goal of regression testing fixture is to decide whether there was a regression or not.
`counters` callback is intended for passing through all 3 RDD's: reference, test and diffs and computing 
basic metrics such as count of diffs, volume of input data, etc. which can help to make the decision of whether there
was indeed regression.
 
Although result assessment can be done outside of regression fixture, having counters as a dedicated callback
serves two purposes: It makes reusing standard ways of regression result assessment easier as well as facilitates 
parallel execution of built-in counters implementation.