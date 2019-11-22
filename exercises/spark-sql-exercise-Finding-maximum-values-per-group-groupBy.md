# Exercise: Finding maximum values per group (groupBy)

Develop a standalone Spark SQL application (using IntelliJ IDEA) that finds the highest (maximum) numbers per group.

Protip™: Use [Dataset.groupBy](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) operator and [max](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard function

Module: **Spark SQL**

Duration: **20 mins**

## Input Dataset

```text
val nums = spark.range(5).withColumn("group", 'id % 2)
scala> nums.show
+---+-----+
| id|group|
+---+-----+
|  0|    0|
|  1|    1|
|  2|    0|
|  3|    1|
|  4|    0|
+---+-----+
```

## Result

```text
+-----+------+
|group|max_id|
+-----+------+
|    0|     4|
|    1|     3|
+-----+------+
```

<!--
## Solution

```text
val solution = nums.groupBy('group).agg(max("id") as "max_id")
```

-->
