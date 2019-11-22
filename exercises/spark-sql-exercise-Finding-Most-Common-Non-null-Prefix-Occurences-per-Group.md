# Exercise: Finding Most Common Non-null Prefix per Group (Occurences)

Write a structured query that finds the most common not-null `PREFIX` (occurences) per `UNIQUE_GUEST_ID`.

Module: **Spark SQL**

Duration: **15 mins**

## Credits

This exercise is brought to you by **Julien**. Merci.

## Input Dataset

```text
+---------------+------+
|UNIQUE_GUEST_ID|PREFIX|
+---------------+------+
|              1|    Mr|
|              1|   Mme|
|              1|    Mr|
|              1|  null|
|              1|  null|
|              1|  null|
|              2|    Mr|
|              3|  null|
+---------------+------+
```

```text
val input = Seq(
  (1, "Mr"),
  (1, "Mme"),
  (1, "Mr"),
  (1, null),
  (1, null),
  (1, null),
  (2, "Mr"),
  (3, null)).toDF("UNIQUE_GUEST_ID", "PREFIX")
```

## Result

```text
+---------------+------+
|UNIQUE_GUEST_ID|PREFIX|
+---------------+------+
|              1|    Mr|
|              2|    Mr|
|              3|  null|
+---------------+------+
```

<!--
## Solution

```text
val counter = udf { (ps: Seq[String]) =>
  if (ps.isEmpty) null
  else ps.groupBy(identity).mapValues(_.size).maxBy(_._2)._1 }
input
  .groupBy("UNIQUE_GUEST_ID")
  .agg(collect_list('prefix) as "ps")
  .withColumn("prefix", counter('ps))
  .select("UNIQUE_GUEST_ID", "PREFIX")
  .orderBy("UNIQUE_GUEST_ID")
```
-->
