package com.github.fabianmurariu.sparkws.ex1

import com.github.fabianmurariu.sparkws.BaseSparkSpec

/*
nums
+---+-----+
| id|group|
+---+-----+
|  0|    0|
|  1|    1|
|  2|    0|
|  3|    1|
|  4|    0|
+---+-----+
 */
class Exercise3 extends BaseSparkSpec {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val nums = Seq(
    (0, 1),
    (1, 0),
    (2, 0),
    (3, 1),
    (4, 0),
    (5, 1),
    (6, 0),
    (7, 1),
    (8, 0),
    (9, 0),
  ).toDF("value", "group").cache

  "Ex3" should "count all the distinct items in a group and collect them in a sequence" in {

    val solution = nums.groupBy("group")
      .agg(countDistinct($"value"), collect_list($"value"))
      .sort("group")
      .as[(Long, Long, collection.Seq[Long])]
      .collect()

    solution shouldBe Vector(
      (0L, 6L, List(9, 1, 8, 6, 2, 4)),
      (1L, 4L, List(0, 7, 3, 5))
    )

  }
  /*
  +--------------+-----------+
  |         words|   solution|
  +--------------+-----------+
  |[hello, world]|hello world|
  |[awesome, workshop]|awesome workshop|
  +--------------+-----------+
   */
  it should "concatenate all words in an array to a sequence" in {
    val data = Seq(
      (1, collection.Seq("hello", "world")),
      (2, collection.Seq("awesome", "workshop"))
    ).toDF("id", "words")

    val solution = data
      .withColumn("solution", concat_ws(" ", $"words"))
      .as[(Long, collection.Seq[String], String)]
      .collect()

    solution shouldBe Vector(
      (1, List("hello", "world"), "hello world"),
      (2, List("awesome", "workshop"), "awesome workshop")
    )
  }
}
