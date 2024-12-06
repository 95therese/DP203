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