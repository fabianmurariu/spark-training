# Exercise: Difference in Days Between Dates As Strings

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that calculates the number of days between dates given as text (in some format) and the current date.

Protip™: Use the standard functions for date and time, e.g. [to_date](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) and [datediff](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$)

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
val dates = Seq(
   "08/11/2015",
   "09/11/2015",
   "09/12/2015").toDF("date_string")
scala> dates.show
+-----------+
|date_string|
+-----------+
| 08/11/2015|
| 09/11/2015|
| 09/12/2015|
+-----------+
```

## Result

```text
+-----------+----------+----+
|date_string|   to_date|diff|
+-----------+----------+----+
| 08/11/2015|2015-11-08|1256|
| 09/11/2015|2015-11-09|1255|
| 09/12/2015|2015-12-09|1225|
+-----------+----------+----+
```

**NOTE**: The `diff` column is `current_date`-sensitive and your result will certainly be different (as the current date of yours will be after the page was published).

<!--
## Solution

```text
val solution = dates
   .withColumn("to_date", to_date('date_string, "dd/MM/yyyy"))
   .withColumn("diff", datediff(current_date, 'to_date))
```

-->
