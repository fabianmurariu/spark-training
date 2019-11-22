# Exercise: Finding Most Populated Cities Per Country

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that gives the most populated cities per country with the population.

Protip™: Use [Dataset.groupBy](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) operator and [max](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard function followed by [Dataset.join](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset).

**NOTE**: `population` column in the input dataset is a string and contains spaces.

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
+-----------------+-------------+----------+
|             name|      country|population|
+-----------------+-------------+----------+
|           Warsaw|       Poland| 1 764 615|
|           Cracow|       Poland|   769 498|
|            Paris|       France| 2 206 488|
|Villeneuve-Loubet|       France|    15 020|
|    Pittsburgh PA|United States|   302 407|
|       Chicago IL|United States| 2 716 000|
|     Milwaukee WI|United States|   595 351|
+-----------------+-------------+----------+
```

```text
name,country,population
Warsaw,Poland,1 764 615
Cracow,Poland,769 498
Paris,France,2 206 488
Villeneuve-Loubet,France,15 020
Pittsburgh PA,United States,302 407
Chicago IL,United States,2 716 000
Milwaukee WI,United States,595 351
```

## Result

```text
+----------+-------------+----------+
|      name|      country|population|
+----------+-------------+----------+
|    Warsaw|       Poland| 1 764 615|
|     Paris|       France| 2 206 488|
|Chicago IL|United States| 2 716 000|
+----------+-------------+----------+
```

<!--
## Solution

```text
val cities = spark.read.option("header", true).csv("cities.csv")
val cities_with_pop_long = cities
  .withColumn("pop", translate('population, " ", "") cast "long")
val biggestCitiesPerCountry = cities_with_pop_long
  .groupBy('country)
  .agg(max('pop) as "max_population")
val solution = biggestCitiesPerCountry
  .join(cities_with_pop_long, "country")
  .where('max_population === 'pop)
  .select('name, 'country, 'population)
```
-->
