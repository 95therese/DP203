# 3. Perform Data Engineering with Azure Synapse Apache Spark Pools

## 3.1. Analyse data with Apache Spark in Azure Synapse Analytics

Apache Spark is an open source parallel processing framework for large-scale data processing and analytics. It's very popular in big data scenarios and is available in multiple platform implementations, indlucing Azure HDInsight, Databricks, and Synapse Analytics. Here we explore how to use Spark in Synapse Analytics for ingestion, processing, and analysis of data from a data lake. The core techniques and code here are commin to all Spark implementations.

### 3.1.1. Get to know Apache Spark

Apache Spark is a distributed data processing framework that enables large-scale data analytics by coordinating work across multiple processing nodes in a cluster.

**How Spark works:** Spark applications runs as independent sets of processes on a cluster, coordinated by the *SparkContext* object in our main program, aka the driver program. The SparkContext connects to the cluster manager, which allocates resources accros applications using an implementation of Apache Hadoop YARN. Once connected, Spark acquires executors on nodes in the cluster to run the application code.

The SparkContext runs the main function and parallel operations on the cluster nodes, and then collects the results of the operations. The nodes read and write data from and to the file system cache transformed data in-memory as *Resilient Distributed Datasets* (RDDs).

The SparkContext converts an application to a *directed acyclic graph* (DAG), which consists of individual tasks that get executed within an executor process on the nodes. Each application has its own executor process, which remains up while the application is running.

**Spark pools in Azure Synapse Analytics:** In Synapse Analytics, clusters are implemented as a *Spark pool*, which provides a runtime for Spark operations. We can create Spark pools by using the Azure portal or Synapse Studio. When defining a Spark pool, we can specify configuration options like:
- Name for the pool
- The size of the VM used for the nodes in the pool
- The number of nodes in the pool, and whether the pool size is fixed or individual nodes can be brought online dynamically to auto-scale the cluster. Can also specify maximum or minimum number of active nodes
- Version of the Spark Runtime to be used, this dictates the versions of individual components (e.g. Python, Java, etc.) that get installed

Spark pools are serverless, so they start on-demand and stop when idle.


### 3.1.2. Use Spark in Azure Synapse Analytics

We can run many different applications on Spark, including code in Python or Scala scripts, Java code compiled as a Java Archive (JAR), and more. Spark is commonly used for two kinds of workload:
- Batch or stream processing to ingest, clean, and transform data. Often done as part of an automated pipeline.
- Interactive analytics sessions to explore, analyse, and visualise data.

**Running Spark code in notebooks:** Synapse Studio has an integrated notebook interface that enables working with Spark. Notebooks are often used interactively, but can also be including in automated pipelines or run as an unattended script.

**Accessing data from a Synapse Spark pool:** We can use Spark in Synapse Analytics to work with data from various sources, including:
- a data lake on the primary storage account for the Azure Synapse Analytics workspace
- a data lake based on storage defined as a linked service in the workspace
- a dedicated or serverless SQL pool in the workspace
- an Azure SQL or SQL Server database (using the Spark connector for SQL Server)
- an Azure Cosmos DB analytical database defined as a linked service and configured using Azure Synapse Link for Cosmos DB
- an Azure Data Explorer Kusto database defined as a linked service in the workspace
- an external Hive metastore defined as a linked service in the workspace

One of the most common uses of Spark is working with data in a data lake.

### 3.1.3. Analyse data with Spark

A benefit of Spark is support for several programming languages. The default language in a new notebook is PySpark, a version of Python optimised for Spark that is commonly used by data scientists and analysts. We can also use Scala and SQL. And software engineers can use frameworks like Java and Microsoft .NET to create solutions that run on Spark.

**Exploring data with dataframes:** While Spark uses RDD as its native data structure and we can work directly with these, it's more common in Spark to work with dataframes. Dataframes are provided as part of the Spark SQL library and are similar to dataframes from the Pandas library, but optimised for Spark's environment. Spark SQL also has Dataset API that is supported in Java and Scala.

**Loading data into a dataframe:** Suppose we have a file named **products.csv** in the primary storage account for an Azure Synapse Analytics workspace:

``` CSV
ProductID,ProductName,Category,ListPrice
771,"Mountain-100 Silver, 38",Mountain Bikes,3399.9900
772,"Mountain-100 Silver, 42",Mountain Bikes,3399.9900
773,"Mountain-100 Silver, 44",Mountain Bikes,3399.9900
...
```

In a Spark notebook, we can load this data into a dataframe and display the first 10 rows with PySpark as follows:

``` Python
%%pyspark
df = spark.read.load('abfss://containter@store.dfs.core.windows.net/products.csv',
     format='csv',
     header=True) 

display(df.limit(10))
```

The equivalent code in Scala is this:

``` Scala
%%spark
val df = spark.read.format("csv").option("header","true").load("abfss://container@store.dfs.core.windows.net/products.csv")

display(df.limit(10))
```

**Specifying a dataframe schema:** We can also explicitly specify a schema for the data, for example when there are no column names in the data file. If our data file is called **product-data.csv**, we can do this with PySpark as follows:

``` Python
from pyspark.sql.types import *
from pyspark.sql.functions import *

productSchema = StructType([
    StructField("ProductID", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Category", StringType()),
    StructField("ListPrice", FloatType())
    ])

df = spark.read.load('abfss://container@store.dfs.core.windows.net/product-data.csv',
    format='csv',
    schema=productSchema,
    header=False)

display(df.limit(10))
```

**Filtering and grouping dataframes:** The Dataframe class has methods for filtering, sorting, grouping, and otherwise manipulating its data. For example, we can use the **select** method to get the columns of the datafram:

``` Python
pricelist_df = df.select("ProductID", "ListPrice")
```

The result is another dataframe object. An alternative notation for the same operation:

``` Python
pricelist_df = df["ProductID", "ListPrice"]
```

We can chain together methods to do several manipulations in one line of code, for example:

``` Python
bikes_df = df.select("ProductName","ListPrice").where((df["Category"]=="Mountain Bikes") | (df["Category"]=="Road Bikes"))
```

We also have functions for grouping and aggregating data, for example:

``` Python
counts_df = df.select("ProductID","Category").groupBy("Category").count()
```

**Using SQL expressions in Spark:** The Dataframe API is part of the Spark SQL library, so we have ways of using SQL in Spark to work with the data.

The Spark catalogue is a metastore for relational data objects like views and tables. The Spark runtime can use the catalogue to integrated code in any Spark-supported language with SQL expressions.

If we want to make data in a dataframe available for querying in the Spark catalogue, we can make a temporary view, such as:

``` Python
df.createOrReplaceTempView("products")
```

As views are temporary, the are deleted at the end of the session. We can use tables that are persisted in the catalogue to define a database that can be queried using Spark SQL.

Now that **products** is a view, we can query it, for example, with PySpark to get data returned as a dataframe:

``` Python
bikes_df = spark.sql("SELECT ProductID, ProductName, ListPrice \
                      FROM products \
                      WHERE Category IN ('Mountain Bikes', 'Road Bikes')")
```

We can also query objects in the catalogue with the notebook as follows:

``` SQL
&&sql

SELECT Category, COUNT(ProductID) AS ProductCount
FROM products
GROUP BY Category
ORDER BY Category
```

### 3.1.4. Visualise data with Spark

We can visualise results of queries as charts using notebooks or graphics packages, for example Python libraries.

**Using built-in notebook charts:** The results of SQL queries in Spark notebooks are displayed right under the appropriate code cell. The default is to render the results as a table, but there are also options there for view the results as a chart along with some customisation options for the chart.

This is useful when the results of a query doesn't include any existing groupings or aggregations, and we want to quickly summarise the data visually. To have more control over the format of the data or to display values that are already aggreagted in the query, it's better to use a graphics package and create our own visualisations.

**Using graphics packages in code:** There are many graphics packages that can do data visualisation. Notably, Python supports many packages, most of them built on the **Matplotlib** library. The output can be rendered in a notebook, making it easy to work with the data with inline data visualisations and then use markdown cells for providing commentary.

For example, some PySpark code that creates a chart from some aggregated data with Matplotlib can look like this:

``` Python
from matplotlib import pyplot as plt

# Get the data as a Pandas dataframe
data = spark.sql("SELECT Category, COUNT(ProductID) AS ProductCount \
                  FROM products \
                  GROUP BY Category \
                  ORDER BY Category").toPandas()

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(12,8))

# Create a bar plot of product counts by category
plt.bar(x=data['Category'], height=data['ProductCount'], color='orange')

# Customize the chart
plt.title('Product Counts by Category')
plt.xlabel('Category')
plt.ylabel('Products')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=70)

# Show the plot area
plt.show()
```

Note that Matplotlib requires the data to be in a Pandas dataframe rather than a Spark dataframe, so we need the **toPandas** method. We have other libraries that can create charts, for example **Seaborn**.


## 3.2. Transform data with Spark in Azure Synapse Analytics

Spark can be used not just for analytics, but also for transforming large amounts of data. With the Spark dataframe, we can load data from a data lake and do complex modifications, then we can save the result back to the lake for downstream processing or ingestion into a data warehouse.

Synapse Analytics provides Apache Spark pools where we can run Spark workloads and use natively supported notebooks to write and run Spark code. Can then use other Synapse Analytics tools, like SQL pools, to work with the transformed data.

### 3.2.1. Modify and save dataframes

Recall that we load data into a dataframe with the **spark.read** function, where we specify the file format, file path, and optionally further options, for example:

``` Python
order_details = spark.read.csv('/orders/*.csv', header=True, inferSchema=True)
display(order_details.limit(5))
```

**Transform the data structure:** Once the data is loaded into a dataframe, some common operations we can do on it include filtering rows and columns, renaming columns, creating new columns (often derived from existing ones), and replacing null or other values.

For example, to modify how we handle the customer's name:

``` Python
from pyspark.sql.functions import split, col

# Create the new FirstName and LastName fields
transformed_df = order_details.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName", " ").getItem(1)))

# Remove the CustomerName field
transformed_df = transformed_df.drop("CustomerName")

display(transformed_df.limit(5))
```

**Save the transformed data:** When we're done transforming some data, we can save the result to a supported format in the data lake. For example, to save the dataframe to a parquet file and replace any existing files with the same name:

```Python
transformed_df.write.mode("overwrite").parquet('/transformed_data/orders.parquet')
print("Transformed data saved!")
```

The Parquet format is good for data files that will be used for further analysis or ingestion into an analytical store. It's an efficient format supported by most large scale data analytics systems. In fact, sometimes the data transformation requirement can be to just convert data from a format (e.g. CSV) into Parquet.

### 3.2.2. Partition data files

Partitioning is an optimisation technique that enables spark to maximise performance across the worker nodes. More performance gains can be achieved when filtering data in queries by eliminating unnecessary disk IO.

**Partition to the output file:** To save a dataframe as a set of files, we use the **partitionBy** method and we pick a column whose values determine which partition each row is stored in. Example:

``` Python
from pyspark.sql.functions import year, col

# Load source data
df = spark.read.csv('/orders/*.csv', header=True, inferSchema=True)

# Add Year column
dated_df = df.withColumn("Year", year(col("OrderDate")))

# Partition by year
dated_df.write.partitionBy("Year").mode("overwrite").parquet("/data")
```

The generated folder names will include the column name and the value in the **column=value** format. For example, the folder containing the files with *Year* value *2020* will be named *Year=2020*.

We have the option of partitioning data by multiple columns, which creates a hierarchy of folders. So could, for example, partition by year and month.

**Filter parquet files in a query:** When reading data from parquet files, we can pull data from any folder within the hierarchical folders. For example:

``` Python
orders_2020 = spark.read.parquet('/partitioned_data/Year=2020')
display(orders_2020.limit(5))
```

The partition columns specified in the file path will be omitted in the resulting dataframe. In this example, if the *Year* column was included, all the rows would have value *2020*.

### 3.2.3. Transform data with SQL

We can also query and transform data in dataframes with SQL queries and persist the results as tables. By tables here, we mean the metadata abstraction over the files in the data lake, not an actual relational talbe.

**Define tables and views:** Table definitions in Spark are stored in the *metastore*. And *external* tables are relational tables in the metastore that reference files in a data lake location that we specify. We access the data by querying the table or by reading the files directly from the data lake.

External tables are loosely bound to the underlying files, so deleting a table does not delete the associated files. This means we can use Spark for transforming and persisting data, then delete the table while downstream processes still can access the transformed data.

We can also define *managed* tables for which the underlying data files are stored in an internally managed storage location with the metastore. Managed tables are tightly-bound to the files, so dropping a managed table will delete the associated files.

Example where a dataframe is saved as an external table:

``` Python
order_details.write.saveAsTable('sales_orders', format='parquet', mode='overwrite', path='/sales_ordesr_table')
```

**Use SQL to query and transform the data:** After a table is created , we can use SQL to query and transform the data:

``` Python
# Create derived columns
sql_transform = spark.sql("SELECT *, YEAR(OrderDate) AS Year, MONTH(OrderDate) AS Month FROM sales_orders")

# Save the results
sql_transform.write.partitionBy("Year","Month").saveAsTable('transformed_orders', format='parquet', mode='overwrite', path='/transformed_orders_table')
```

**Query the metastore:** As the table is created in the metastore, we can use SQL directly in the notebook with the **%%sql** magic key, for example:

``` SQL
%%sql

SELECT * FROM transformed_orders
WHERE Year = 2021
    AND Month = 1
```

**Drop tables:** We can use the **DROP** command to drop external tables without affecting the files in the data lake.

``` SQL
%%sql

DROP TABLE transformed_orders;
DROP TABLE sales_orders;
```

## 3.3. Use Delta Lake in Azure Synapse Analytics

Delta Lake enables relational database capabilities for batch streaming data and allows implementing a data lakehouse architecture in Spark. The version of Delta Lake available in a Synapse Analytics pool depends on the verison of Spark specified in the pool configuration. The information here reflects Delta Lake version 1.0, which is installed in Spark 3.1.

### 3.3.1. Understand Delta Lake

Delta Lake is an open-source storage layer that adds relational database semantics to Spark-based data lake processing. Delta Lake is supported in Azure Synapse Analytics Spark pools for PySpark, Scala, and .NET code.

Benefits of using Delta Lake in a Synapse Analytics Spark pool include:

- *Relational tables that support querying and data modification:* can store data in tables that support CRUD operations, like in a traditional relational database system.
- *Support for ACID transactions:* Delta Lake does this by implementing a transaction log and enforces serialisable isolation for concurrent operations.
- *Data versioning and time travel:* because of the transaction log, we can track multiple versions of each table row and can access time travel features to retrieve previous versions of a row in a query.
- *Support for batch and stremaing data:* Spark supports streaming data through the Spark Structured Streaming API. Delta Lake tables can be used as both sources and sinks for streaming data.
- *Standard formats and interoperability:* the underlying data is stored in the Parquet format. We can also use the serverless SQL pool in Synapse Analytics to query Delta Lake tables in SQL.

### 3.3.2. Create Delta Lake tables

Delta Lake is built on tables, which provide a relational storage abstraction over files in a data lake.

**Creating a Delta Lake table from a dataframe:** Can easily create a Delta Lake table by saving a dataframe in the *delta* format and specifying where the data files and related metadata for the table should be stored. Example with PySpark:

``` Python
# Load a file into a dataframe
df = spark.read.load('/data/mydata.csv', format='csv', header=True)

# Save the dataframe as a delta table
delta_table_path = "/delta/mydata"
df.write.format("delta").save(delta_table_path)
```

The specified path location will contain parquet files for the data (regarless of the format of the source file) and a **_delta_log* folder containing the transaction log for the table.

We can replace an existing Delta Lake table with the contents of a dataframe with the **overwrite** mode.

``` Python
new_df.write.format("delta").mode("overwrite").save(delta_table_path)
```

We can add rows from a dataframe to an existing table with the **append** mode.

``` Python
new_rows_df.write.format("delta").mode("append").save(delta_table_path)
```

**Making conditional updates:** Instead of modifying a dataframe and then replacing a Delta Lake table by overwriting it, it's more common to modify an existing table as discrete transactional operations. We can do this to a Delta Lake table with the **DeltaTable** object from the Delta Lake API, which supports update, delete, and merge operations. For example, to update the **price** column for the rows with a **category** value of "Accessories":

``` Python
from delta.tables import *
from pyspark.sql.functions import *

# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)

# Update the table (reduce price of accessories by 10%)
deltaTable.update(
    condition = "Category == 'Accessories'",
    set = {"Price": "Price * 0.9"})
```

The data modification is recorded in the transaction log and new parquet files are created in the table folder as needed.

**Querying a previous version of a table:** We can use the transaction log to query previous versions of a table. We do this by reading the data into a dataframe, specifying the version with the **versionAsOf** option.

``` Python
df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
```

Alternatively, we can specify a timestamp with the **timestampAsOf** option.

``` Python
df = spark.read.format("delta").option("timestampAsOf", '2022-01-01').load(delta_table_path)
```

### 3.3.3. Create catalogue tables

We can also define Delta Lake tables as catalogue tables in the Hive metastore for our Spark pool, and work with them using SQL. Tables in a Spark catalogue can be managed or external. The differences are:

- *Managed*: defined without a specified location, data files are stored within the storage used by the metastore, dropping tables removes the metadata and the folder with the data files.
- *External*: defined for a custom file location, where data for the table is stored, the metadata is defined in the Spark catalogue, dropping tables deletes metadata but not the data files.

**Creating catalogue tables:** There are multiple ways of doing this, such as from a dataframe, with the **saveAsTable** operation:

``` Python
# Save a dataframe as a managed table
df.write.format("delta").saveAsTable("MyManagedTable")

## Specify a path option to save as an external table
df.write.format("delta").option("path", "/mydata").saveAsTable("MyExternalTable")
```

We can do it with SQL, either with the SparkSQL API:

``` Python
spark.sql("CREATE TABLE MyExternalTable USING DELTA LOCATION '/mydata'")
```

Or with the native SQL support in Spark:

``` SQL
%%sql

CREATE TABLE MyExternalTable
USING DELTA
LOCATION '/mydata'
```

Note that **CREATE TABLE** returns an error if a table with the specified name already exists in the catalogue. Can mitigate this behaviour with **CREATE TABLE IF NOT EXISTS** or **CREATE OR REPLACE TABLE**.

When creating a new managed or external table with a currently empty location, there are nothing to inherit the schema from, so we can specify the schema as part of **CRETE TABLE**:

``` SQL
%%sql

CREAT TABLE ManagedSalesOrders
(
    Orderid INT NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    CustomerName STRING,
    SalesTotal FLOAT NOT NULL
)
USING DELTA
```

With Delta Lake, table schemas are enforced. So inserts and updates must comply with the column nullability and data types.

We can also create catalogue tables with the DeltaTableBuilder API.

``` Python
from delta.tables import *

DeltaTable.create(spark) \
    .tableName("default.ManagedProducts") \
    .addColumn("Productid", "INT") \
    .addColumn("ProductName", "STRING") \
    .addColumn("Category", "STRING") \
    .addColumn("Price", "FLOAT") \
    .execute()
```

**create** will also return an error if a table with the specified name already exists, so can opt to use **createIfNotExists** or **createOrReplace**.

**Using catalogue tables:** We use tables libe tables in any SQL-based relational database.

``` SQL
%%sql

SELECT orderid, salestotal
FROM ManagedSalesOrders
```

### 3.3.4. Use Delta Lake with streaming data

Stream processing solutions involve constantly reading a stream of data from a source, optionally processing or manipulating it, then writing the results to a sink.

Spark has native support for streaming data through Spark Structured Streaming, an API based on a boundless dataframe where streaming data is captured for processing. A Spark Structured Streaming dataframe can read data from many different streaming sources, like network ports, file system locations, or real time message brokering services like Azure Event Hubs or Kafka.

Delta Lake tables can serve as sources or sinks. For example, we can capture real time data from an IoT device and write the stream to a Delta Lake table as a sink, enabling querying for viewing the latest streaming data. Or we can read a Delta Table as a streaming source and repor new data as it is added to the table.

**Using Delta Lake as a streaming source:** To store some details from Internet sales orders and create a stream that reads data from the Delta Lake table folder as new data is appended, we can run the following code.

``` Python
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Load a streming dataframe from the Delta Table
stream_df = spark.readStream.format("delta") \
    .option("ignoreChanges", "true") \
    .load("/delta/internetorders")

# Now you can process the streaming data in the dataframe, for example to show it
stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
```

When using the table as a streaming source, only **append** operations can be included in the stream. Data modifications will cause an error unless the **ignoreChanges** or **ignoreDeletes** options are specified.

After reading the data to a streaming dataframe, we can use the Spark Structured Streaming API to process it. Above we just displayed the dataframe, but we could aggregate the data over temporal windows and send the results to a downstream process for near-real-time visualisation.

**Using a Delta Lake table as a streaming sink:** Below we read a stream of data from JSON files in a folder. The JSON data in each file contains the status for an IoT device and new data is added to the stream whenever a file is added to the folder. The input is a boundless dataframe, which is then written in delta format to a folder location for a Delta Lake table.

``` Python
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a stream that reads JSON data from a folder
inputPath = '/streamingdata/'
jsonSchema = StructType([
    StructField("device", StringType(), False),
    StrcutField("status", StringType(), False)
])
stream_df = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

# Write the stream to a delta table
table_path = '/delta/devicetable'
checkpoint_path = '/delta/checkpoint'
delta_stream = stream_df.writeStream.format("delta").option("checkpointLocation", checkpoint_path).start(table_path)
```

The **checkpointLocation** option is used to write a checkpoint file that tracks the state of the stream processing. This enables us to recover from a failure at the point where the stream processing left off.

After the stream processing has started, we can query the Delta Lake tale to which the streaming output is being written to see the latest data.

``` SQL
%%sql

CREATE TABLE DeviceTable
USING DELTA
LOCATION '/delta/devicetable';

SELECT device, status
FROM DeviceTable;
```

We stop the data streaming with the **stop** method.

``` Python
delta_stream.stop()
```

### 3.3.5. Use Delta Lake in a SQL pool

We can also use the serverless SQL pool to query data in Delta Lake tables, but we can not update, insert, or delete data.

**Querying delta formatted files with OPENROWSET:** The serverless SQL pool can read delta format files, hence query Delta Lake tables. This can be useful if we want to use Spark and Delta tables to process a lot of data, but use the SQL pool to run queries for reporting and analysis of the processed data.

``` SQL
SELECT *
FROM OPENROWSET(
    BULK 'https://mystore.dfs.core.windows.net/files/delta/myable/',
    FORMAT = 'DELTA'
    ) AS deltadata
```

We can run this query in a serverless SQL to retrieve the latest data from the table in the specified file location.

We could also create a database and add a data source that encapsulates the location of our Delta Lake data files, for example like:

``` SQL
CREATE DATABASE MyDB
      COLLATE Latin1_General_100_BIN2_UTF8;
GO;

USE MyDB;
GO

CREATE EXTERNAL DATA SOURCE DeltaLakeStore
WITH
(
    LOCATION = 'https://mystore.dfs.core.windows.net/files/delta/'
);
GO

SELECT TOP 10 *
FROM OPENROWSET(
        BULK 'mytable',
        DATA_SOURCE = 'DeltaLakeStore',
        FORMAT = 'DELTA'
    ) as deltadata;
```

When working with Delta Lake data, it's generally best to create databases with a UTF-8 based collation to ensure string compatibility.

**Querying catalogue tables:** The serverless SQL pool has shared access to databases in the Spark metastore, so we can query catalogue tables created with Spark SQL.

``` SQL
-- By default, Spark catalog tables are created in a database named "default"
-- If you created another database using Spark SQL, you can use it here
USE default;

SELECT * FROM MyDeltaTable;
```