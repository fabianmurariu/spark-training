# Exercise: Using Kafka Data Source

Develop a standalone Spark Structured Streaming application (using IntelliJ IDEA) that uses Kafka as the data source and sink.

Please read [KafkaSource](https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-KafkaSource.html) to learn about the data source and what is required to have it available in Spark Structured Streaming applications.

Use `sbt package` and `spark-submit` to run the application.

Module: **Spark Structured Streaming**

Duration: **45 mins**

## Requirements

You will be using [Apache Kafka](http://kafka.apache.org/) as an external data source. Please download and install it first.

You can run a single-broker Kafka cluster using the following commands (in different consoles):

1. `./bin/zookeeper-server-start.sh config/zookeeper.properties`
2. `./bin/kafka-server-start.sh config/server.properties`

With the single-broker Kafka cluster is up and running, use Kafka Console Producer and Consumer tools to produce and consume messages from a Kafka topic.

1. `./bin/kafka-console-producer.sh --broker-list :9092 --topic input`
2. `./bin/kafka-console-consumer.sh --bootstrap-server :9092 --topic input`

### Part 1. Reading Data From Kafka

1. Use `kafka` streaming source and `console` streaming sink
2. Use Kafka Console Producer to send a message
3. Watch the output of the streaming application in the console

### Part 2. Writing Data To Kafka

1. Use `kafka` streaming source and sink
2. Start Kafka Console Consumer that consumes messages that the streaming application produces
3. Use Kafka Console Producer to produce messages to the topic that the streaming application uses to consume messages from

### Part 3. Transforming Data From Kafka

1. Take values (from Kafka records) and add a new column with the values in upper case

<!--
## Solution

```scala
  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate

  import spark.implicits._
  import org.apache.spark.sql.functions._
  val records = spark
    .readStream
    .format("kafka")
    .option("subscribe", "amadeus-input")
    .option("kafka.bootstrap.servers", ":9092")
    .load
    .select('value cast "string")

  // Add a new column with the values in upper case
    .withColumn("value", upper('value))

  records.writeStream
    .format("kafka")
    .option("checkpointLocation", "checkpoint")
    .option("topic", "amadeus-output")
    .option("kafka.bootstrap.servers", ":9092")
    .start
    .awaitTermination
```

-->
