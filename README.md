# Dataset regression tool

Big data applications built with Spark or Hadoop often produce datasets as series of files with no particular guarantee of neither distribution of data samples between the files, nor ordering within a file.
Building regression tooling for such application can be a tedious task, but it can be greatly simplified with this fixture. It uses Spark to find matching samples
in both data sets, generate a human readable diff, detect duplicates and automatically generate basic stats of a regression run.

###Launching regression
The fixture does not rely on underlying data format, but instead uses user defined closures to extract a key and values from a data sample. This allow it to be
agnostic of data type, while being able to support file format specific DSLs for data extraction.

For example, a regression call for a typed dataset may look like
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

For a weakly typed dataset, such as in json format, a similar call may look like this:
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
Note using a DSL for key and value extraction. This could also be done manually like in the first example.

In certain cases we might need different equality conditions for some of data types or fields. For instance, if we only care about the date of the launch,
but not an exact instant, it can be done with an override:
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

Result of regression is returned as `RegressionResult` object containing two filds:

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
 basic metrics such as count of diffs, volume of input data, etc. 
 
 Although result assessment can be done
 outside of regression fixture, having counters as a dedicated callback serves two purposes: It makes reusing standard ways of 
 regression result assessment easier as well as facilitates parallel execution of built-in counters
 implementation.