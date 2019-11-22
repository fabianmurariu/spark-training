# Exercise: Finding Ids of Rows with Word in Array Column

Develop a standalone Spark SQL application (using IntelliJ IDEA) that finds the ids of the rows that have values of one column in an array column.

Protip™: Use [split](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) and [explode](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard functions

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
+---+------------------+-----+
| id|             words| word|
+---+------------------+-----+
|  1|     one,two,three|  one|
|  2|     four,one,five|  six|
|  3|seven,nine,one,two|eight|
|  4|    two,three,five| five|
|  5|      six,five,one|seven|
+---+------------------+-----+
```

```text
id,words,word
1,"one,two,three",one
2,"four,one,five",six
3,"seven,nine,one,two",eight
4,"two,three,five",five
5,"six,five,one",seven
```

## Result

```text
+-----+------------+
|    w|         ids|
+-----+------------+
| five|   [2, 4, 5]|
|  one|[1, 2, 3, 5]|
|seven|         [3]|
|  six|         [5]|
+-----+------------+
```

The word "one" is in the rows with the ids `1`, `2`, `3` and `5`.

The word "seven" is in the row with the id `3`.

<!--
## Solution

```text
val data = spark.read.option("header", true).option("inferSchema", true).csv("words.csv")
val words = data.withColumn("w", explode(split($"words", ",")))
val solution = words
  .join(data.select($"word" as "w"), Seq("w"))
  .groupBy("w")
  .agg(array_sort(collect_set($"id")) as "ids")
  .orderBy("w")
```

-->
