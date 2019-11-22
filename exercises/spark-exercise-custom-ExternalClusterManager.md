# [Spark Core] Developing Custom ExternalClusterManager

## Goal

The goal of the exercise is to register a custom `ExternalClusterManager` so `scalania` master URL is supported and the following works:

```
$ ./bin/spark-shell --master scalania
Error: Master must either be yarn or start with spark, mesos, local
Run with --help for usage help or --verbose for debug output
```

## Steps

1. Create a brand new Scala/sbt project
    * Use the one and only IntelliJ IDEA
    * Project name: `spark-external-cluster-manager`
1. Add `libraryDependencies` for Spark 2.0.0 (RC5)
    * See https://gist.github.com/jborkowski/a77d9cc88067feea24261479afcc4ee1
1. ScalaTest
    * Follow Spark's own  [ExternalClusterManagerSuite](https://github.com/apache/spark/blob/master/core/src/test/scala/org/apache/spark/scheduler/ExternalClusterManagerSuite.scala)

## Hints

1. The custom Cluster Manager should extend [ExternalClusterManager](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/ExternalClusterManager.scala)
2. The cluster manager support is loaded by [ServiceLoader.load](http://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html#load-java.lang.Class-). See [the code in Spark](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/SparkContext.scala#L2524-L2534)
3. See [how and where the loading happens in Spark](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/SparkContext.scala#L2506-L2520).

## Solution

See a complete Scala/sbt project in [spark-external-cluster-manager](../solutions/spark-external-cluster-manager/). It is a work in progress work.
