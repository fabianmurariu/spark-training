# Exercise: Developing Custom Data Source

Develop your own custom data source (e.g. **XXX**).

Module: **Spark SQL**

Duration: **45 mins**

## Result

```scala
spark
  .read
  .format("XXX") // <-- your custom data source / format
  .option("header", true) // <-- the data source supports options
  .load("data.xxx")
  // column pruning
  .select('id, 'name)
  // filter pushdown
  .where('id > 5)
  .show
```

<!--
## Solution

FIXME Link to the repo?

-->
