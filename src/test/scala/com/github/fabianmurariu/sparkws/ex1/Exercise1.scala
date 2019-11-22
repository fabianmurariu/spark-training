package com.github.fabianmurariu.sparkws.ex1

import com.github.fabianmurariu.sparkws.BaseSparkSpec

class Exercise1 extends BaseSparkSpec {

  import spark.implicits._

  /*
   *  spec1.csv
   *  one,true,,1,2019-08-02
   *  two,false,2.3,2,1960-01-13
   *  three,false,4.5,3,1973-02-11
   *  four,true,2.9,4,1983-07-01
   */
  "Ex1" should "read a csv file, not funky options, not even a header, just remember to show the DF" in {
    val path = pathFor("/ex1/spec1.csv")
    val frame = spark.read.csv(path.toString)
    frame.show
    frame.count() shouldBe 4
  }

  /*
   *  left, right
   *  1,a
   *  2,b
   */
  it should "read a csv file with a header by using spark.read.option(_, _) the row count should be 2" in {
    val path = pathFor("/ex1/spec2.csv")
    val frame = spark.read.option("header", "true").csv(path.toString)
    frame.count() shouldBe 2
  }

  /*
   *  left, right
   *  1,a
   *  2,b
   */
  it should "read a csv file and extract the first element as a tuple of (String, String)" in {
    val path = pathFor("/ex1/spec2.csv")
    val frame = spark.read.option("header", "true")
      .csv(path.toString)
      .as[(String, String)]

    frame.count() shouldBe 2
    frame.head shouldBe("1", "a")
  }

  /*
   * spec3.csv
   * ping|pong
   * 1.1|a
   * 2.2|b
   */
  it should "read a csv file with '|' as separators and infer the type to be (Double, String)" in {
    val path = pathFor("/ex1/spec3.csv")
    val ds = spark.read.option("sep", "|")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path.toString)
      .as[(Double, String)]

    ds.count shouldBe 2
    ds.head shouldBe(1.1, "a")
  }

  /*
   *  spec1.csv
   *  one,true,,1,2019-08-02
   *  two,false,2.3,2,1960-01-13
   *  three,false,4.5,3,1973-02-11
   *  four,true,2.9,4,1983-07-01
   */
  it should "read a csv file, and extract the first and second element as a tuple with the correct type" in {
    val path = pathFor("/ex1/spec1.csv")
    val ds = spark.read.option("inferSchema", "true")
      .csv(path.toString)
      .as[(String, Boolean, Option[Double], Int, java.sql.Timestamp)]

    val first :: second :: _ = ds.collect().toList
    first shouldBe("one", true, None, 1, timestamp(2019, 8, 2))
    second shouldBe("two", false, Some(2.3), 2, timestamp(1960, 1, 13))
  }
}
