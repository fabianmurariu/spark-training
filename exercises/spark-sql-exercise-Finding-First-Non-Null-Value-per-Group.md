# Exercise: Finding First Non-Null Value per Group

Write a structured query that finds the first non-null value per group.

Protip™: Review the input arguments of the `first` standard function

Module: **Spark SQL**

Duration: **15 mins**

## Input Dataset

```scala
val data = Seq(
  (None, 0),
  (None, 1),
  (Some(2), 0),
  (None, 1),
  (Some(4), 1)).toDF("id", "group")
```

```text
scala> data.show
+----+-----+
|  id|group|
+----+-----+
|null|    0|
|null|    1|
|   2|    0|
|null|    1|
|   4|    1|
+----+-----+
```

## Result

```text
+-----+--------------+
|group|first_non_null|
+-----+--------------+
|    1|             4|
|    0|             2|
+-----+--------------+
```

<!--
## Solution

```scala
val solution = data.groupBy('group).agg(first('id, ignoreNulls = true) as "first_non_null")
```

-->

## Credits

* [How to find first non-null values in groups? (secondary sorting using dataset api)](https://stackoverflow.com/q/42986965/1305344)
