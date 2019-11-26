package com.github.fabianmurariu.sparkws

import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, LocalDate, LocalDateTime}

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

trait BaseSparkSpec extends FlatSpec with Matchers {

  implicit lazy val spark = SparkSession.builder().appName("spark-workshop-exercise")
    .master("local[1]")
    .getOrCreate()

  def pathFor(relative: String): Path = {
    val path = Paths.get(this.getClass.getResource(relative).toURI).toAbsolutePath
    assert(Files.exists(path), s"relative: ${relative}, absolute: ${path} does not exist")
    path
  }

  def date(year:Int, month:Int, day:Int):java.sql.Date = {
    java.sql.Date.valueOf(LocalDate.of(year, month, day))
  }

  def timestamp(year:Int, month:Int, day:Int, hour:Int = 0, min:Int=0, second:Int=0):java.sql.Timestamp = {
    java.sql.Timestamp.valueOf(LocalDateTime.of(year, month, day, hour, min, second))
  }

  def instant(year: Int, month:Int, day:Int) =
    Instant.ofEpochSecond(1)
}
