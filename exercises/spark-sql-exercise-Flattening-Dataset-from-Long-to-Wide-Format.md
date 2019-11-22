# Exercise: Flattening Dataset from Long to Wide Format

The exercise is to pivot an input dataset in long format to wide format.

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
+---+----+----+----+
|key|val1|val2|date|
+---+----+----+----+
| k1|  v4|  v7|  d1|
| k1|  v5|  v8|  d2|
| k1|  v6|  v9|  d3|
| k2| v12| v22|  d1|
| k2| v32| v42|  d2|
| k2| v11| v21|  d3|
+---+----+----+----+
```

```text
key,val1,val2,date
k1,v4,v7,d1
k1,v5,v8,d2
k1,v6,v9,d3
k2,v12,v22,d1
k2,v32,v42,d2
k2,v11,v21,d3
```

## Expected Dataset

```text
+---+-----+-----+-----+-----+-----+-----+
|key|d1_v1|d1_v2|d2_v1|d2_v2|d3_v1|d3_v2|
+---+-----+-----+-----+-----+-----+-----+
| k1|   v4|   v7|   v5|   v8|   v6|   v9|
| k2|  v12|  v22|  v32|  v42|  v11|  v21|
+---+-----+-----+-----+-----+-----+-----+
```

<!--
val input = spark.read.option("header", true).csv("input.csv)
val solution = input.groupBy('key).pivot('date).agg(first('val1) as "v1", first('val2) as "v2").orderBy('key)
-->

## Hints

* Use `pivot` operator with multiple aggregations
* Use `as` operator to get the expected column names

## Credits

* Based on [How to flatten long dataset to wide format (pivot) with no join?](https://stackoverflow.com/q/43349932/1305344)
