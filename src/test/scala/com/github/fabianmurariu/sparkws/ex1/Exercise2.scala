package com.github.fabianmurariu.sparkws.ex1

import com.github.fabianmurariu.sparkws.BaseSparkSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

/*
 Select various columns and filter, do simple count and sum aggregations

+-----------------+-------------+----------+
|             name|      country|population|
+-----------------+-------------+----------+
|           Warsaw|       Poland| 1 764 615|
|           Cracow|       Poland|   769 498|
|            Paris|       France| 2 206 488|
|Villeneuve-Loubet|       France|    15 020|
|    Pittsburgh PA|United States|   302 407|
|       Chicago IL|United States| 2 716 000|
|     Milwaukee WI|United States|   595 351|
+-----------------+-------------+----------+
 */
class Exercise2 extends BaseSparkSpec {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  lazy val cities = citiesDataFrame.cache()

  "Ex2" should "read a CSV with information about cities and only display rows with population greater than 2 million" in {
    val rows@(first :: second :: _) = cities.filter($"population" > 2000000)
      .as[(String, String, Long)]
      .collect()
      .toList

    assert(rows.length == 2)
    assert(first._3 > 2000000)
    assert(second._3 > 2000000)
  }

  it should "find the distinct list of countries" in {
    val countries = cities.select("country").distinct().cache()

    countries.count() shouldBe 3
    countries.as[String].collect().sorted shouldBe Vector("France", "Poland", "United States")
  }

  it should "sum up the total population of the most populated cities for the entire dataset" in {

    val solution = cities.groupBy("country")
      .agg(max("population").as("population"))
      .select(sum("population"))
      .as[Long]
      .head()

    solution shouldBe 6687103

  }

  it should "add a column with all the names of the cities in capital letters named upper_name" in {
    val first :: second :: _  = cities.withColumn("upper_name", upper($"name"))
      .as[(String, String, Long, String)]
      .collect().toList

    first shouldBe("Warsaw", "Poland", 1764615, "WARSAW")
    second shouldBe("Cracow", "Poland", 769498, "CRACOW")
  }

  it should "use join to find if a city is a capital" in {
    val capitals = Seq(
      ("Washington DC", "United States"),
      ("Warsaw", "Poland"),
      ("Paris", "France")).toDF("capital", "country")

    val result = cities.join(capitals, Seq("country"), "left_outer")
      .withColumn("is_capital", $"name" === $"capital")
      .select("name", "is_capital")
      .as[(String, Boolean)]
      .collect()

    result shouldBe Seq(
      ("Warsaw",true),
      ("Cracow",false),
      ("Paris",true),
      ("Villeneuve-Loubet",false),
      ("Pittsburgh PA",false),
      ("Chicago IL",false),
      ("Milwaukee WI",false))
  }

  it should "find the most populated cities per country" in {
    // join solution no window
//    val cities_with_pop_long = cities
//      .withColumn("pop", translate('population, " ", "") cast "long")
//    val biggestCitiesPerCountry = cities_with_pop_long
//      .groupBy('country)
//      .agg(max('pop) as "max_population")
//    val solution = biggestCitiesPerCountry
//      .join(cities_with_pop_long, "country")
//      .select('name, 'country, 'population)
//      .as[(String, String, Long)]
//      .collect().sorted

    val w = Window.partitionBy("country").orderBy($"population".desc)
    val solution = cities.select($"name", $"country", $"population", rank().over(w).as("rank"))
        .filter("rank = 1")
        .drop("rank")
        .as[(String, String, Long)]
        .collect().sorted

    solution shouldBe Vector(
      ("Chicago IL", "United States", 2716000),
      ("Paris", "France", 2206488),
      ("Warsaw", "Poland", 1764615)
    )
  }

  private def citiesDataFrame = {
    val citiesPath = pathFor("/ex2/spec1.csv")
    val df = spark.read
      .option("inferSchema", "true")
      .option("sep", "|")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("header", "true").csv(citiesPath.toString)
    df
  }
}
