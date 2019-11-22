# Exercise: Email Classification

Develop a Spark MLlib application that uses [Logistic Regression](https://en.wikipedia.org/wiki/Logistic_regression) for email classification, i.e. what emails are spam and not a spam.

Module: **Spark MLlib**

Duration: **45 mins**

## Steps

1. Use Spark MLlib's [LogisticRegression](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.classification.LogisticRegression) and the transformers: [Tokenizer](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.Tokenizer) and [HashingTF](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.HashingTF)
2. Use Spark MLlib's [Pipeline](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.Pipeline)
3. Load CSV datasets for training and as a raw data
4. Use Spark MLlib's [CrossValidator](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tuning.CrossValidator) for model selection
5. Persist the (best) model
6. Calculate predictions
    1. Display the values, i.e. `0` is `OK` while `1` is `SPAM`, using `when` standard function
    2. `val status = when('prediction === 0, "OK").otherwise("SPAM").as("status")`

## Input Dataset

Use [Online Generate Test Data](http://www.convertcsv.com/generate-test-data.htm) to generate a CSV dataset with fake emails and the columns: `id`, `body`, and `label`.

```text
id,body,label
1,Zushad zam fawo gur licidtug zar honepru zolor muahada lep pired ciuvi.,0
2,Elfi ez lirde vizavbak depmapav us piwojaw sihhib novo luzkut de teb apemimi hezotce rubumzer mowja jowte.,1
```

<!--
## Solution

val solution = ???
-->
