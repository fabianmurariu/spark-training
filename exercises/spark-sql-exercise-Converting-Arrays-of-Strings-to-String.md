# Exercise: Converting Arrays of Strings to String

Based on [How to convert column of arrays of strings to strings?](https://stackoverflow.com/q/38924762/1305344).

Module: **Spark SQL**

## Input Dataset

```text
val words = Seq(Array("hello", "world")).toDF("words")
scala> words.show
+--------------+
|         words|
+--------------+
|[hello, world]|
+--------------+

scala> words.printSchema
root
 |-- words: array (nullable = true)
 |    |-- element: string (containsNull = true)
```

## Expected Dataset

```text
scala> solution.show
+--------------+-----------+
|         words|   solution|
+--------------+-----------+
|[hello, world]|hello world|
+--------------+-----------+

scala> solution.printSchema
root
 |-- words: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- solution: string (nullable = false)
```

Duration: **15 mins**

## Protips

1. Use [concat_ws](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) function
    * Concatenates multiple input string columns together into a single string column, using the given separator

<!--
## Solution

val solution = words.withColumn("solution", concat_ws(" ", $"words"))
-->
