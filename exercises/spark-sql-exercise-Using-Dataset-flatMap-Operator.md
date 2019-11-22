# Exercise: Using Dataset.flatMap Operator

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that creates as many rows as the number of elements in a given array column. The values of the new rows should be the elements of the array column themselves.

Protip™: Use [Dataset.flatMap](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) operator

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

Please note that the output has two columns (not one!)

## Useful Links

1. Scaladoc of the [Dataset API](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)

<!--
## Solution

```text
// The following give one-column output only
val partSol1 = nums.flatMap(r => r.getSeq[Int](0))
val partSol2 = nums.flatMap(r => r.getSeq(0): Seq[Int])
val partSol3 = nums.flatMap(r => r.getAs[Seq[Int]]("nums"))

val solution = nums.as[Seq[Int]].flatMap((ns: Seq[Int]) => ns.map(n => (ns, n))).toDF("nums", "num")
val solution = for {
  ns <- nums.as[Seq[Int]]
  n <- ns
} yield (ns, n)
```

-->
