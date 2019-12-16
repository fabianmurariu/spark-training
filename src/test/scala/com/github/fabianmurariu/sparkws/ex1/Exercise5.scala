package com.github.fabianmurariu.sparkws.ex1

import com.github.fabianmurariu.sparkws.BaseSparkSpec
import org.apache.spark.sql.DataFrame

import scala.util.Random

/*
  The exercise could also be described as
  "Calculating the gap between the current book and the bestseller per genre"
  (given the other exercise with book sales and bestsellers).

  id,title,genre,quantity
  1,Hunter Fields,romance,15
  2,Leonard Lewis,thriller,81
  3,Jason Dawson,thriller,90
  4,Andre Grant,thriller,25
  5,Earl Walton,romance,40
  6,Alan Hanson,romance,24
  7,Clyde Matthews,thriller,31
  8,Josephine Leonard,thriller,1
  9,Owen Boone,sci-fi,27
  10,Max McBride,romance,75
*/
class Exercise5 extends BaseSparkSpec {
  import spark.implicits._
  import org.apache.spark.sql.functions.max

  "Exercise5" should "calculate the bestseller per genre" in {
    val sales = loadBookSales

    val result = sales.groupBy("genre")
      .agg(max("quantity").as("max"))
      .as[(String, Long)]
      .collect()

    result shouldBe Vector("romance" -> 75L, "thriller" -> 90L, "sci-fi" -> 27L)

  }

  it should "get ratings for the book and normalize them" in {
    val reviewersScale = Seq(
      "metacritic" -> 10,
      "bookreviews" -> 5
    ).toDF("reviewer", "max_score")

    val generatedRatings =
      (1 to 10).map(bookId => (bookId, "metacritic", Random.nextInt(10) + 1)) ++
      (1 to 10).map(bookId => (bookId, "bookreviews", Random.nextInt(5) + 1))

    val ratings = generatedRatings.toDF("book_id", "reviewer", "score")

    val result = ratings.join(reviewersScale, Seq("reviewer"))
      .withColumn("norm_score", $"score" / $"max_score")
      .select("book_id", "norm_score")
      .as[(String, Double)]
      .collect()

    val max = result.maxBy(_._2)._2
    val min = result.minBy(_._2)._2
    assert(max <= 1.0d && max >= 0.0d)
    assert(min >= 0.0d && min <= 1.0d )
  }

  def loadBookSales:DataFrame = {
    spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(pathFor("/ex5/spec1.csv").toString)
  }
}
