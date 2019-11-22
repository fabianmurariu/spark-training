# Exercise: Finding Longest Sequence (Window Aggregation)

Write a structured query that finds the longest sequence of consecutive numbers.

Protip™: Use `rank` standard function followed by `Dataset.groupBy` operator to `count` the same ranks

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
ID,time
1,1
1,2
1,4
1,7
1,8
1,9
2,1
3,1
3,2
3,3
```

```text
val visits = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("visits.csv")
scala> visits.show
+---+----+
| ID|time|
+---+----+
|  1|   1|
|  1|   2|
|  1|   4|
|  1|   7|
|  1|   8|
|  1|   9|
|  2|   1|
|  3|   1|
|  3|   2|
|  3|   3|
+---+----+
```

## Result

```text
+---+----+
| ID|time|
+---+----+
|  1|   3|
|  2|   1|
|  3|   3|
+---+----+
```

<!--
## Solution

```scala
import org.apache.spark.sql.expressions.Window

val solution = ???
```

-->

## Credits

* [How to find longest sequence of consecutive dates?](https://stackoverflow.com/q/44282077/1305344)
