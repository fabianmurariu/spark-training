# Exercise: Using pivot for Cost Average and Collecting Values

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that calculates cost average ([part 1](#part-1-result)) and collects all the values in a column ([part 2](#part-2-result)).

Protip™: Use [RelationalGroupedDataset.pivot](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset) operator

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
+---+----+----+------+----+
| id|type|cost|  date|ship|
+---+----+----+------+----+
|  0|   A| 223|201603|PORT|
|  0|   A|  22|201602|PORT|
|  0|   A| 422|201601|DOCK|
|  1|   B|3213|201602|DOCK|
|  1|   B|3213|201601|PORT|
|  2|   C|2321|201601|DOCK|
+---+----+----+------+----+
```

```scala
val data = Seq(
  (0, "A", 223, "201603", "PORT"),
  (0, "A", 22, "201602", "PORT"),
  (0, "A", 422, "201601", "DOCK"),
  (1, "B", 3213, "201602", "DOCK"),
  (1, "B", 3213, "201601", "PORT"),
  (2, "C", 2321, "201601", "DOCK")
).toDF("id","type", "cost", "date", "ship")
```

## Part 1. Result

```text
+---+----+------+------+------+
| id|type|201601|201602|201603|
+---+----+------+------+------+
|  0|   A| 422.0|  22.0| 223.0|
|  1|   B|3213.0|3213.0|  null|
|  2|   C|2321.0|  null|  null|
+---+----+------+------+------+
```

<!--
data.groupBy("id", "type").pivot("date").agg(avg("cost")).orderBy('id).show
-->

## Part 2. Result

```text
+---+----+------+------+------+
| id|type|201601|201602|201603|
+---+----+------+------+------+
|  0|   A|[DOCK]|[PORT]|[PORT]|
|  1|   B|[PORT]|[DOCK]|    []|
|  2|   C|[DOCK]|    []|    []|
+---+----+------+------+------+
```

<!--
data.groupBy("id", "type").pivot("date").agg(collect_set("ship")).orderBy('id).show
-->

## Credits

* [How to use pivot and calculate average on a non-numeric column (facing AnalysisException "is not a numeric column")?](https://stackoverflow.com/q/37486910/1305344)
