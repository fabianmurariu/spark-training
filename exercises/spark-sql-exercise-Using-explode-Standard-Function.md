# Exercise: Using explode Standard Function

Develop a standalone Spark SQL application (using IntelliJ IDEA) that creates a new row for every element in the given array column.

Use web UI to compare performance (query plans) of queries with `explode` standard function and `Dataset.flatMap` operator.

Think about the differences between `explode` function and `flatMap` operator. Are there any? What are they? Can you generate new rows? How many?

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
val nums = Seq(Seq(1,2,3)).toDF("nums")

scala> nums.printSchema
root
 |-- nums: array (nullable = true)
 |    |-- element: integer (containsNull = false)


scala> nums.show
+---------+
|     nums|
+---------+
|[1, 2, 3]|
+---------+
```

## Result

```text
+---------+---+
|     nums|num|
+---------+---+
|[1, 2, 3]|  1|
|[1, 2, 3]|  2|
|[1, 2, 3]|  3|
+---------+---+
```

## Useful Links

1. Scaladoc of the [functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) object

<!--
## Solution

```text
val solution = nums.withColumn("num", explode('nums))
```

-->
