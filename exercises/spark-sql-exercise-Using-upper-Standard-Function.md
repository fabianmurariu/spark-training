# Exercise (Mohamed): Using upper Standard Function

Develop a standalone Spark SQL application (using IntelliJ IDEA) that converts one or more string columns to upper case.

The standalone application should take _at least_ two input parameters:

* The path of a CSV data set to load
* One or more column names

The output dataset should extend the current columns with new ones with their names including the "source", e.g. if `city` column were used, the output could be `upper_city`.

Extra: Make sure the conversion happens on string columns only

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
+---+-----------------+-------+
| id|             city|country|
+---+-----------------+-------+
|  0|           Warsaw| Poland|
|  1|Villeneuve-Loubet| France|
|  2|           Vranje| Serbia|
|  3|       Pittsburgh|     US|
+---+-----------------+-------+
```

```text
id,city,country
0,Warsaw,Poland
1,Villeneuve-Loubet,France
2,Vranje,Serbia
3,Pittsburgh,US
```

## Result

```text
+---+-----------------+-------+-----------------+
| id|             city|country|       upper_city|
+---+-----------------+-------+-----------------+
|  0|           Warsaw| Poland|           WARSAW|
|  1|Villeneuve-Loubet| France|VILLENEUVE-LOUBET|
|  2|           Vranje| Serbia|           VRANJE|
|  3|       Pittsburgh|     US|       PITTSBURGH|
+---+-----------------+-------+-----------------+
```

## Useful Links

1. Scaladoc of the [functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) object

<!--
## Solution

```text
val cities = spark.read.option("header", true).csv("cities.csv")
val solution = cities.withColumn("upper_city", upper('city))
```

-->
