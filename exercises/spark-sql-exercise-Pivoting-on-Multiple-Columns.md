# Exercise: Pivoting on Multiple Columns

Write a structured query that pivots a dataset on multiple columns. Since `pivot` aggregation allows for a single column only, find a solution to pivot on two or more columns.

Protip™: Use `RelationalGroupedDataset.pivot` and `Dataset.join` operators

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
+---+---+-----+-----+
| id|day|price|units|
+---+---+-----+-----+
|100|  1|   23|   10|
|100|  2|   45|   11|
|100|  3|   67|   12|
|100|  4|   78|   13|
|101|  1|   23|   10|
|101|  2|   45|   13|
|101|  3|   67|   14|
|101|  4|   78|   15|
|102|  1|   23|   10|
|102|  2|   45|   11|
|102|  3|   67|   16|
|102|  4|   78|   18|
+---+---+-----+-----+
```

```scala
val data = Seq(
  (100,1,23,10),
  (100,2,45,11),
  (100,3,67,12),
  (100,4,78,13),
  (101,1,23,10),
  (101,2,45,13),
  (101,3,67,14),
  (101,4,78,15),
  (102,1,23,10),
  (102,2,45,11),
  (102,3,67,16),
  (102,4,78,18)).toDF("id", "day", "price", "units")
```

## Result

```text
+---+-------+-------+-------+-------+------+------+------+------+
| id|price_1|price_2|price_3|price_4|unit_1|unit_2|unit_3|unit_4|
+---+-------+-------+-------+-------+------+------+------+------+
|100|     23|     45|     67|     78|    10|    11|    12|    13|
|101|     23|     45|     67|     78|    10|    13|    14|    15|
|102|     23|     45|     67|     78|    10|    11|    16|    18|
+---+-------+-------+-------+-------+------+------+------+------+
```

Think how you can avoid multiple scans over the input dataset.

<!--
val daily_prices = data.withColumn("daily_price", concat(lit("price_"), $"day"))
val prices = daily_prices.groupBy("id").pivot("daily_price").agg(first($"price"))

val daily_units = data.withColumn("daily_unit", concat(lit("unit_"), $"day"))
val units = daily_units.groupBy("id").pivot("daily_unit").agg(first($"units"))

val solution = prices.join(units, "id").orderBy("id")
-->

## Credits

* [How to pivot on multiple columns in Spark SQL?](https://stackoverflow.com/q/45035940/1305344)
