# Exercise: Specifying Table and SQL Query on Command Line

Develop a standalone Spark SQL application that registers a temporary view and uses command-line arguments for the table name and the SQL query.

Module: **Spark SQL**

Duration: **30 mins**

## Steps

1. Use **args** for the table name and the SQL query
    * **args(0)** is the table name
    * **args(1)** is the query to be executed
2. Register the dataset as a temporary view
    * Use **createOrReplaceTempView**
3. Execute the SQL query using **spark.sql**
