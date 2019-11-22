# Exercise: Calculating aggregations

Develop a standalone Spark SQL application (using IntelliJ IDEA) that calculates aggregations defined on a command line (e.g. finds the biggest city among the cities in a dataset).

Protip™: Use [Dataset.agg](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) operator and [standard functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) only (not UDFs!)

The standalone application should take _at least_ two input parameters:

* The path of a CSV data set to load
* One or more aggregations (e.g. `max`, `avg`)

Protip™: Mind the spaces in `population` column and then the type.

Extra: Include the name of the city when one aggregation is used.

Module: **Spark SQL**

Duration: **20 mins**

## Input Dataset

```text
+---+-----------------+----------+
| id|             name|population|
+---+-----------------+----------+
|  0|           Warsaw| 1 764 615|
|  1|Villeneuve-Loubet|    15 020|
|  2|           Vranje|    83 524|
|  3|       Pittsburgh| 1 775 634|
+---+-----------------+----------+
```

```text
id,name,population
0,Warsaw,1 764 615
1,Villeneuve-Loubet,15 020
2,Vranje,83 524
3,Pittsburgh,1 775 634
```

## Result

```text
+----------+
|population|
+----------+
|   1775634|
+----------+
```

<!--
## Solution

```text
val cities = spark.read.option("header", true).csv("cities.csv")

val solution1 = cities
  .withColumn("pop", translate($"population", " ", "") cast "long")
  .agg(max('pop) as "population")

val solution2 = cities.withColumn("pop", regexp_replace('population, " ", "").as[Int]).agg(max('pop))

val solution3 = cities.withColumn("pop", regexp_replace('population, "\\s+", "").as[Int]).agg(max('pop))
```

-->
