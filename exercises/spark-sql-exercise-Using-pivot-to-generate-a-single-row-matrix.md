# Exercise: Using pivot to generate a single-row matrix

Write a structured query that transposes (pivots) a two-column dataframe to a single-row one (long to wide).

Protip™: Use `RelationalGroupedDataset.pivot` operator

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
+--------+-----+
|   udate|   cc|
+--------+-----+
|20090622|  458|
|20090624|31068|
|20090626|  151|
|20090629|  148|
|20090914|  453|
+--------+-----+
```

```text
udate,cc
20090622,458
20090624,31068
20090626,151
20090629,148
20090914,453
```

## Result

```text
+-----+--------+--------+--------+--------+--------+
|udate|20090622|20090624|20090626|20090629|20090914|
+-----+--------+--------+--------+--------+--------+
|   cc|     458|   31068|     151|     148|     453|
+-----+--------+--------+--------+--------+--------+
```

<!--
https://stackoverflow.com/q/40370890/1305344
## Solution

```text
val solution = d.groupBy().pivot("udate").agg(first("cc"))
val betterSolution = solution.select(lit("cc") as "udate", '*)
```

-->
