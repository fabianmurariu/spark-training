# Exercise: Generating Exam Assessment Report

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that generates an exam assessment report (given the answers from multiple students and their attempts) that includes the question IDs (as columns) and the answers (as their values).

Please note that a student (participant) can answer one or more questions in a single assessment and geo tag.

Protip™: Use [RelationalGroupedDataset.pivot](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset) operator

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
+---+-------------+----------+-------------+----------+-------+
|Qid|     Question|AnswerText|ParticipantID|Assessment| GeoTag|
+---+-------------+----------+-------------+----------+-------+
|  1|Question1Text|       Yes|       abcde1|         0|(x1,y1)|
|  2|Question2Text|        No|       abcde1|         0|(x1,y1)|
|  3|Question3Text|         3|       abcde1|         0|(x1,y1)|
|  1|Question1Text|        No|       abcde2|         0|(x2,y2)|
|  2|Question2Text|       Yes|       abcde2|         0|(x2,y2)|
+---+-------------+----------+-------------+----------+-------+
```

```text
Qid,Question,AnswerText,ParticipantID,Assessment,GeoTag
1,Question1Text,Yes,abcde1,0,"(x1,y1)"
2,Question2Text,No,abcde1,0,"(x1,y1)"
3,Question3Text,3,abcde1,0,"(x1,y1)"
1,Question1Text,No,abcde2,0,"(x2,y2)"
2,Question2Text,Yes,abcde2,0,"(x2,y2)"
```

## Result

```text
+-------------+----------+-------+-----+-----+-----+
|ParticipantID|Assessment| GeoTag|Qid_1|Qid_2|Qid_3|
+-------------+----------+-------+-----+-----+-----+
|       abcde1|         0|(x1,y1)|  Yes|   No|    3|
|       abcde2|         0|(x2,y2)|   No|  Yes| null|
+-------------+----------+-------+-----+-----+-----+
```

<!--
val input = spark.read.option("header", true).option("inferSchema", true).csv("input.csv)
val inputWithHeaders = input.withColumn("header", concat(lit("Qid_"), $"Qid"))
val solution = inputWithHeaders.groupBy('ParticipantID, $"Assessment", $"GeoTag").pivot('header).agg(first('AnswerText))
-->

## Credits

* [How to pivot on arbitrary column?](https://stackoverflow.com/q/47720822/1305344)
