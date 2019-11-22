# Exercise: Calculating percent rank

A dataset has employees and salaries entries in no particular order. Write a structured query that adds a new column per the following requirements:

1. Top 30% gets a value "high"
2. The next 40% gets "average"
3. The rest gets "low"

Protip™: Use [percent_rank](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) window aggregation followed by [when](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard function with the Column API's [when](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column) and [otherwise](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column) methods.

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
Employee,Salary
Tony,50
Alan,45
Lee,60
David,35
Steve,65
Paul,48
Micky,62
George,80
Nigel,64
John,42
```

```text
val salaries = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("salaries.csv")
scala> salaries.show
+--------+------+
|Employee|Salary|
+--------+------+
|    Tony|    50|
|    Alan|    45|
|     Lee|    60|
|   David|    35|
|   Steve|    65|
|    Paul|    48|
|   Micky|    62|
|  George|    80|
|   Nigel|    64|
|    John|    42|
+--------+------+
```

## Result

```text
+--------+------+----------+
|Employee|Salary|Percentage|
+--------+------+----------+
|  George|    80|      High|
|   Steve|    65|      High|
|   Nigel|    64|      High|
|   Micky|    62|      High|
|     Lee|    60|   Average|
|    Tony|    50|       Low|
|    Paul|    48|       Low|
|    Alan|    45|       Low|
|    John|    42|       Low|
|   David|    35|       Low|
+--------+------+----------+
```

## Credits

* [How to add a column with values based on popularity?](https://stackoverflow.com/q/47195372/1305344)

<!--
## Solution

import org.apache.spark.sql.expressions.Window
val bySalary = Window.orderBy('salary.desc)

val ranks = salaries.withColumn("rank", percent_rank over bySalary)

val labeling = when('rank < 0.35, "High").when('rank < 0.45, "Average").otherwise("Low")
val solution = ranks.withColumn("Percentage", labeling).select('Employee, 'Salary, 'Percentage)
-->
