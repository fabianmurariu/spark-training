# Exercise: Calculating Gap Between Current And Highest Salaries Per Department

Write a structured query that shows the difference in salaries between the top-paid employee and others per department. In other words, we want to know how much more the highest-paid employee gets compared to other teammates.

The exercise could also be described as "Calculating the gap between the current book and the bestseller per genre" (given the other exercise with book sales and bestsellers).

Protip™: Use [max](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) or [first](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard functions

Module: **Spark SQL**

Duration: **15 mins**

## Input Dataset

```text
id,name,department,salary
1,Hunter Fields,IT,15
2,Leonard Lewis,Support,81
3,Jason Dawson,Support,90
4,Andre Grant,Support,25
5,Earl Walton,IT,40
6,Alan Hanson,IT,24
7,Clyde Matthews,Support,31
8,Josephine Leonard,Support,1
9,Owen Boone,HR,27
10,Max McBride,IT,75
```

```text
val salaries = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("salaries.csv")
scala> salaries.show
+---+-----------------+----------+------+
| id|             name|department|salary|
+---+-----------------+----------+------+
|  1|    Hunter Fields|        IT|    15|
|  2|    Leonard Lewis|   Support|    81|
|  3|     Jason Dawson|   Support|    90|
|  4|      Andre Grant|   Support|    25|
|  5|      Earl Walton|        IT|    40|
|  6|      Alan Hanson|        IT|    24|
|  7|   Clyde Matthews|   Support|    31|
|  8|Josephine Leonard|   Support|     1|
|  9|       Owen Boone|        HR|    27|
| 10|      Max McBride|        IT|    75|
+---+-----------------+----------+------+
```

NOTE: Use [Online Generate Test Data](http://www.convertcsv.com/generate-test-data.htm) for more sophisticated datasets in CSV or JSON format.

## Result

```text
+---+-----------------+----------+------+----+
| id|             name|department|salary|diff|
+---+-----------------+----------+------+----+
|  9|       Owen Boone|        HR|    27|   0|
|  1|    Hunter Fields|        IT|    15|  60|
|  5|      Earl Walton|        IT|    40|  35|
|  6|      Alan Hanson|        IT|    24|  51|
| 10|      Max McBride|        IT|    75|   0|
|  2|    Leonard Lewis|   Support|    81|   9|
|  3|     Jason Dawson|   Support|    90|   0|
|  4|      Andre Grant|   Support|    25|  65|
|  7|   Clyde Matthews|   Support|    31|  59|
|  8|Josephine Leonard|   Support|     1|  89|
+---+-----------------+----------+------+----+
```

## Questions

1. How does `orderBy` influence the result? Why?
    1. Think about the number of rows included in a window (mind `rangeBetween`)

<!--
## Solution

```text
import org.apache.spark.sql.expressions.Window

// Solution 1
val departmentById = Window
  .partitionBy("department")
  .orderBy("id")
  .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
val solution = salaries.withColumn("diff", (max('salary) over departmentById) - 'salary)

// Solution 2
val departmentBySalaryDesc = Window.partitionBy("department").orderBy($"salary".desc)
val solution = salaries
  .orderBy("id")
  .withColumn("diff", (max('salary) over departmentBySalaryDesc) - 'salary)

// You could use first instead (when with orderBy!)
val solution = salaries
  .orderBy("id")
  .withColumn("diff", (first('salary) over departmentBySalaryDesc) - 'salary)
```

-->
