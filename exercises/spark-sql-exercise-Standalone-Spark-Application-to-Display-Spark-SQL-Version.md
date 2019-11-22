# Exercise: Standalone Spark Application to Display Spark SQL Version

The exercise is to create a standalone Spark SQL application in Scala that displays the version of Spark SQL in use.

Module: **Spark SQL**

Duration: **30 mins**

## Steps

1. In **IntelliJ IDEA** create a new Scala sbt-managed project
2. Define Spark SQL dependency in **build.sbt**
    * **libraryDependencies**
    * **"org.apache.spark" %% "spark-sql" % "2.4.1"**
3. Write the required **Spark SQL code**
    * Create a new instance of [SparkSession](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession)
    * Use [SparkSession.version](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession@version:String) method to know the version of Spark on which this application is running
    * **spark.version**
4. Build an executable jar
    * **sbt package**
5. Run the application
    * **spark-submit**
