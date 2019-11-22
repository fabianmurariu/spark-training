# Exercise: Running Spark Applications on Hadoop YARN

1. Download Apache Hadoop
2. Start a single-node YARN cluster
3. `spark-submit` a Spark application to YARN
    1. Use the [binary distribution of Spark](http://spark.apache.org/downloads.html) with YARN support, i.e. "Pre-built for Apache Hadoop 2.7 and later"
    2. Use `run-example SparkPi`
    3. Ensure that `HADOOP_CONF_DIR` or `YARN_CONF_DIR` point to the directory with (client-side) configuration files for the Hadoop cluster
4. Use [YARN UI](http://localhost:8088)

Duration: 30 mins

## Useful Links

1. [Running Spark on YARN](https://spark.apache.org/docs/latest/running-on-yarn.html)