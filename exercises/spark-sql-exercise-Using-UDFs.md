# Exercise: Using UDFs

Develop a standalone Spark SQL application (using IntelliJ IDEA) that uses your own `upper` user-defined function (e.g. `my_upper`).

Protip™: Use Scala's [StringOps.toUpperCase](https://www.scala-lang.org/api/current/scala/collection/immutable/StringOps.html)

Use your UDF in SQL, i.e. in `spark.sql`.

Use [callUDF](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard function to call your UDF.

Module: **Spark SQL**

Duration: **30 mins**

## Nondeterministic UDFs

Think about using non-deterministic "features" like the current timestamp or a random number. What happens when you use such "features" in your UDFs?

```scala
// Use .asNondeterministic to see the change
val my_date = udf { (n: Long) => util.Random.nextInt() }
spark
  .range(1)
  .withColumn("randgen", randgen('id))
  .select(randgen('id) === 'randgen)
  .show
```

<!--
## Solution

```text
???
```

-->
