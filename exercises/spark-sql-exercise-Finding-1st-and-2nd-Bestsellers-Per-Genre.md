# Exercise: Finding 1st and 2nd Bestsellers Per Genre

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that gives the 1st and 2nd bestsellers per genre.

Protip™: Use [rank](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard function

Module: **Spark SQL**

Duration: **15 mins**

## Input Dataset

```text
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
```

```text
val books = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("books.csv")
scala> books.show
+---+-----------------+--------+--------+
| id|            title|   genre|quantity|
+---+-----------------+--------+--------+
|  1|    Hunter Fields| romance|      15|
|  2|    Leonard Lewis|thriller|      81|
|  3|     Jason Dawson|thriller|      90|
|  4|      Andre Grant|thriller|      25|
|  5|      Earl Walton| romance|      40|
|  6|      Alan Hanson| romance|      24|
|  7|   Clyde Matthews|thriller|      31|
|  8|Josephine Leonard|thriller|       1|
|  9|       Owen Boone|  sci-fi|      27|
| 10|      Max McBride| romance|      75|
+---+-----------------+--------+--------+
```

NOTE: Use [Online Generate Test Data](http://www.convertcsv.com/generate-test-data.htm) for more sophisticated datasets in CSV or JSON format.

## Result

```text
+---+-------------+--------+--------+----+
| id|        title|   genre|quantity|rank|
+---+-------------+--------+--------+----+
| 10|  Max McBride| romance|      75|   1|
|  5|  Earl Walton| romance|      40|   2|
|  3| Jason Dawson|thriller|      90|   1|
|  2|Leonard Lewis|thriller|      81|   2|
|  9|   Owen Boone|  sci-fi|      27|   1|
+---+-------------+--------+--------+----+
```

<!--
## Solution

```text
import org.apache.spark.sql.expressions.Window
val genreByQuantityDesc = Window.partitionBy("genre").orderBy($"quantity".desc)
val solution = books.withColumn("rank", rank over genreByQuantityDesc).filter($"rank" < 3)
```

-->
