# Exercise: Streaming CSV Datasets

Develop a standalone Spark Structured Streaming application (using IntelliJ IDEA) that runs a streaming query that loads CSV files and prints their content out to the console.

The query should use `csv` streaming source and `console` streaming sink.

Use `sbt package` and `spark-submit` to run the application.

Module: **Spark Structured Streaming**

Duration: **30 mins**

<!--
## Solution

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object StreamingCsvApp extends App {

  val spark = SparkSession.builder.getOrCreate

  case class Person(id: Long, name: String)

  import org.apache.spark.sql.Encoders
  val schema = Encoders.product[Person].schema

  import scala.concurrent.duration._
  val q = spark
    .readStream
    .option("header", true)
    .schema(schema)
    .csv("csv-input/*.csv")
    .writeStream
    .format("console")
    .outputMode(OutputMode.Append)
    .queryName("from-csv-to-console")
    .trigger(Trigger.ProcessingTime(5.seconds))
    .start

  q.awaitTermination()
  spark.stop()
}
```
-->
