# Exercise: Collect values per group

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that collects ids per group in a dataset.

Protip™: Use [collect_list](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard function

Extra: The values collected should be ordered in a descending order

Module: **Spark SQL**

Duration: **15 mins**

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
+-----+---------+
|group|      ids|
+-----+---------+
|    0|[0, 2, 4]|
|    1|   [1, 3]|
+-----+---------+
```

<!--
## Solution

```text
val solution = nums.groupBy("group").agg(collect_list('id) as "ids")
```

-->
