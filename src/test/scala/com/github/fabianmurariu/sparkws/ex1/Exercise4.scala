package com.github.fabianmurariu.sparkws.ex1

import com.github.fabianmurariu.sparkws.BaseSparkSpec

/*
+---+------------------+-----+
| id|             words| word|
+---+------------------+-----+
|  1|     one,two,three|  one|
|  2|     four,one,five|  six|
|  3|seven,nine,one,two|eight|
|  4|    two,three,five| five|
|  5|      six,five,one|seven|
+---+------------------+-----+
 */
class Exercise4 extends BaseSparkSpec {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  /*
  Solution should be

  +-----+------------+
  |    w|         ids|
  +-----+------------+
  | five|   [2, 4, 5]|
  |  one|[1, 2, 3, 5]|
  |seven|         [3]|
  |  six|         [5]|
  +-----+------------+
   */
  "Ex4" should "find ids of rows with Word in array column" in {

    val data = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(pathFor("/ex4/spec1.csv").toString)

    val words = data.withColumn("w", explode(split($"words", ",")))

    val solution = words
      .join(data.select($"word" as "w"), Seq("w"))
      .groupBy("w")
      .agg(array_sort(collect_set($"id")) as "ids")
      .orderBy("w")
      .as[(String, collection.Seq[Long])]

    solution.collect() shouldBe Vector(
      ("five", List(2, 4, 5)),
      ("one", List(1, 2, 3, 5)),
      ("seven", List(3)),
      ("six", List(5))
    )
  }
}
