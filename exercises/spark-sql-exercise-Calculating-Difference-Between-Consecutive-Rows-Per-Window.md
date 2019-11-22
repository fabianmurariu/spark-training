# Exercise: Calculating Difference Between Consecutive Rows Per Window

Write a structured query that calculates the difference between consecutive `running_total` rows over time per department.

Protip™: Use [lag](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard function

Module: **Spark SQL**

Duration: **15 mins**

## Input Dataset

```text
time,department,items_sold,running_total
1,IT,15,15
2,Support,81,81
3,Support,90,171
4,Support,25,196
5,IT,40,55
6,IT,24,79
7,Support,31,227
8,Support,1,228
9,HR,27,27
10,IT,75,154
```

```text
// Mind the inferSchema so time is a numeric value
val sales = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("sales.csv")
scala> sales.show
+----+----------+----------+-------------+
|time|department|items_sold|running_total|
+----+----------+----------+-------------+
|   1|        IT|        15|           15|
|   2|   Support|        81|           81|
|   3|   Support|        90|          171|
|   4|   Support|        25|          196|
|   5|        IT|        40|           55|
|   6|        IT|        24|           79|
|   7|   Support|        31|          227|
|   8|   Support|         1|          228|
|   9|        HR|        27|           27|
|  10|        IT|        75|          154|
+----+----------+----------+-------------+
```

NOTE: Use [Online Generate Test Data](http://www.convertcsv.com/generate-test-data.htm) for more sophisticated datasets in CSV or JSON format.

## Result

```text
// ordering (time) does not really matter
// but helps reviewing the output
scala> solution.show
+----+----------+----------+-------------+----+
|time|department|items_sold|running_total|diff|
+----+----------+----------+-------------+----+
|   9|        HR|        27|           27|  27|
|   1|        IT|        15|           15|  15|
|   5|        IT|        40|           55|  40|
|   6|        IT|        24|           79|  24|
|  10|        IT|        75|          154|  75|
|   2|   Support|        81|           81|  81|
|   3|   Support|        90|          171|  90|
|   4|   Support|        25|          196|  25|
|   7|   Support|        31|          227|  31|
|   8|   Support|         1|          228|   1|
+----+----------+----------+-------------+----+
```

<!--
## Solution

```text
import org.apache.spark.sql.expressions.Window
val departmentByTimeAsc = Window.partitionBy("department").orderBy($"time".asc)

val solution = sales.withColumn("diff", 'running_total - lag('running_total, 1, 0).over(departmentByTime))
```

-->
