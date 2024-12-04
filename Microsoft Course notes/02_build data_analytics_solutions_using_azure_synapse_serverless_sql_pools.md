# 2. Build Data Analytics Solutions Using Azure Synapse Serverless SQL Pools

As prerequisites for this learning path, Microsoft says to consider completing the following learning paths:
- [Explore data analytics in Azure](https://learn.microsoft.com/en-us/training/paths/azure-data-fundamentals-explore-data-warehouse-analytics/?azure-portal=true), which is part of the DP-900 syllabus. So if you've done DP-900, you've likely already done this.
- [Get Started Querying with Transact-SQL](https://learn.microsoft.com/en-us/training/paths/get-started-querying-with-transact-sql/). Something to consider if you're new to T-SQL (or to SQL in general), or if you want some practice with T-SQL. It's a bit long, though

> Note to self: update when this section is done how necessary/useful the T-SQL path is.

> Important note: as of writing this (04/12/24), there's a wrong link in the official Microsoft course. In the module that corresponds to section 2.2.2, the link to the lab sends you to the 2.2.3 lab. The correct link is [here](https://microsoftlearning.github.io/dp-203-azure-data-engineer/Instructions/Labs/02-Analyze-data-with-sql.html).

## 2.1. Use Azure Synapse serverless SQL pool to query files in a data lake

> This module is also part of the *Implement a Data Analytics Solution with Azure Synapse Analytics* learning path, see section 5.2. The notes for this module will be contained here.

This module explores serverless SQL pools, a feature of Azure Synapse Analytics that allows us to use SQL code to query data in files or various other formats without having to load the data into a database storage.

### 2.1.1. Understand Azure Synapse serverless SQL pool capabilities and use cases

Azure Synapse SQL is a distributed query system in Synapse Analytics that offers two kinds of runtime environments:
- Serverless SQL pool: on-demand SQL query processing, primarily used to work with data in a data lake.
- Dedicated SQL pool: enterprise-scale relational database instances that is used to host data warehouses with data stored in relational tables.

Will focus here on serverless SQL pool, which has a pay-per-query endpoint. Benefits of serverless SQL pooL:
- Uses familiar T-SQL syntax
- Integrated connectivity from several BI and ad-hoc querying tools
- Distributed query processing built for large-scale data, and computational functions, thus we get fast query performance
- Built-in query execution fault-tolerance, which gives high reliability and success rates even for long-running queries with large data sets
- No infrastructure to setup or clusters to maintain, so can start querying as soon as the workspace is created
- No charge for resources reserved, only for the data processed by queries you run

**When to use serverless SQL pools:** Serverless SQL pools are tailored for querying data in a data lake, so we don't need to think about data ingestion. Can just query the data in the lake directly.

Serverless SQL pools are good for unplanned or 'bursty' workloads, and they are good for tracking and managing costs of querying.

As serverless SQL pool is built for analytics, it is NOT recommended for OLTP workloads, i.e. workloads where you want millisecond response times and want to pinpoint a single row in a data set.

Common use cases for serverless SQL pools:
- **Data exploration:** browsing the data lake for initial insights, can be done with Azure Synapse Studio. Can use serverless SQL pool to automatically generate SQL scripts for selecting TOP 100 rows from a file or folder. Can then apply common operations like projections, filtering, grouping, etc.
- **Data transformation:** serverless SQL pool can do SQL-based data transformation, both interactively or as part of an automated data pipeline.
- **Logical data warehouse:** can define external objects like tables and views in a serverless SQL database. The data is still stored in the data lake files, but they are also abstractedd by a relational schema that allows client applications and analytical tools to query the data is if they were in a relational database in SQL Server.

### 2.1.2. Query files using a serverless SQL

With a serverless SQL pool, can query data files in various formats, including delimited text (like CSV), JSON, and Parquet. The syntax is the same and is based on the OPENROWSET function in SQL. For example:

``` SQL
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.csv',
    FORMAT = 'csv') AS rows;
```

OPENROWSET has more parameters that can determine things like the schema of the resulting rowset and additional formatting options for delimited text files. See the Azure Synapse Analytics documentation for the full syntax.

The output of OPENROWSET is a rowset that needs to be assigned an alias (*rows* in the example above). The BULK parameter contains the full URL to the location in the data lake that contains the data files. FORMAT specifies the type of data being queried. The example above reads all .csv files in the *files* folder. The example assumes that the user has access to the files in the underlying store.

Clearly, we can use wildcards in the BULK parameter. Other examples of what the URL can look like:
- `https://mydatalake.blob.core.windows.net/data/files/file1.csv `: includes only *file1.csv*
- `https://mydatalake.blob.core.windows.net/data/files/file*.csv`: all .csv files that start with 'file'
- `https://mydatalake.blob.core.windows.net/data/files/*`: all files in the *files* folder
- `https://mydatalake.blob.core.windows.net/data/files/**`: all files in the *files* folder and recursively its subfolders

We can also state multiple file paths in BULK by separating them with commas.

**Querying delimited text files:** The formatting in delimited files can vary, for example a file can be with or without a header row, values are delimited by comma or tabs, endings are Windows or Unix style, and there can be non-quoted and quoted values as well as escaping characters. In all cases, we still read the file with OPENROWSET by setting the FORMAT to 'csv' and handle the formatting details with other parameters. Some relevant parameters:

- PARSER_VERSION: string that determines how the query interprets the text encoding used in the files. Version 1.0 is default and supports a wide range of file encodings, and version 2.0 has fewer encodings but better performance.
- FIRSTROW: integer that states how many rows to skip, for eliminating unstructured preamble test or ignoring column headings.
- FIELDTERMINATOR: determines the characters used to separate field values. Default is comma (,), but can also use TAB (\t), for example.
- ROWTERMINATOR: character for siginfying the end of a row. For example, '\n'.
- FIELDQUOTE: character for encolsing quoted string values, default is double quote ("). If there is a comma in a field that we don't want to be interpreted as a field delimiter, we enclose the whole field value in double quotes, for example an address might be written as "126 Main St, apt 2".

It's common for delimited text files to have the column names in the first row of the file. We can get OPENROWSET to automatically use the first row to set the column names and infer the data types of the columns based on their values, and we do this by setting the parameter HEADER_ROW to TRUE. This is only available for parser version 2.0.

We can manually set column names and data types by including a WITH clause, for example:

``` SQL
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.csv',
    FORMAT = 'csv')
WITH (
    product_id INT,
    product_name VARCHAR(20) COLLATE Latin1_General_100_BIN2_UTF8,
    list_price DECIMAL(5,2)
) AS rows;
```

**Querying JSON files:** To get data from JSON files, an appropriate SQL can look like the following:

``` SQL
SELECT doc
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.json',
    FORMAT = 'csv',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b',
    ROWTERMINATOR = '0x0b',
) WITH (doc NVARCHAR(MAX)) as rows;
```

Since OPENROWSET doesn't have JSON as an option for FORMAT, we instead use csv as the format with FIELDTERMINATOR, FIELDQUOTE, and ROWTERMINATOR set to '0x0b' and a schema with a single NVARCHAR(MAX) column. The result if a rowset with a single column of JSON documents, like:

|doc|
|---|
|{"product_id":123,"product_name":"Widget","list_price": 12.99}|
|{"product_id":124,"product_name":"Gadget","list_price": 3.99}|

And to extract individual values, use the JSON_VALUE function in the SELECT statement:

``` SQL
SELECT JSON_VALUE(doc, '$.product_name') AS product,
       JSON_VALUE(doc, '$.list_price') AS price
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.json',
    FORMAT = 'csv',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b',
    ROWTERMINATOR = '0x0b',
) WITH (doc NVARCHAR(MAX)) as rows;
```

The output would then be:

| product | price |
|   ---   |  ---  |
| Widget  | 12.99 |
| Gadget  | 3.99  |

**Querying Parquet files:** Parquet is commonly used for big data processing on distributed files storage. It's an efficient format optimised for compressing and analytical querying. The schema of the data is often embedded within the file, so we just set the FORMAT parameter to *parquet*, as follows:

``` SQL
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/files/*.*'
    FORMAT = 'parquet') AS rows;
```

**Query partitioned data:** It's common that data in a data lake is partitioned across multiple files in subfolders. For example, sales order data might be partitioned into folders denoting years, and each year folder has a subfolder for each month. If we wanted to include only the orders for January and February 2020, the we can use the code:

``` SQL
SELECT *
FROM OPENROWSET(
    BULK 'https://mydatalake.blob.core.windows.net/data/orders/year=*/month=*/*.*',
    FORMAT = 'parquet') AS orders
WHERE orders.filepath(1) = '2020'
    AND orders.filepath(2) IN ('1','2');
```

### 2.1.3. Create external database objects

We can use OPENROWSET to do queries in the default database of the serverless SQL pool to explore data in the data lake. But sometimes we might want to have a custom database to make it easier to work with data we query frequently.

**Creating a database:** Can be done with Synapse Studio or with a CREATE DATABASE. Might need to set collation appropriately. For example:

``` SQL
CREATE DATABASE SalesDB
    COLLATE Latin1_General_100_BIN2_UTF8;
```

**Creating an external data source:** If we plan to query data in the same location frequently, we can define an external data source to reference that location. For example:

``` SQL
CREATE EXTERNAL DATA SOURCE files
WITH (
    LOCATION = 'https://mydatalake.blob.core.windows.net/data/files'
)
```

This can simplify OPENROWSET queries quite a bit, for example:

``` SQL
SELECT *
FROM OPENROWSET (
    BULK 'orders/*.csv',
    DATA_SOURCE = 'files',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0' ) AS orders
```

The DATA_SOURCE points to the appropriate *files* folder, and the BULK parameter then specifies the relative file path within the *files* folder. So here, we get all the .csv files in the *orders* folder, which is in *files*.

Another benefit of data sources, is that we can assign credentials for the data source, allowing users to query the data without having to permit them to access the data directly in the storage account. For example, we create a credential that uses a shared access signature (SAS) to authenticate against the underlying Azure storage account that hosts the data lake as follows:

``` SQL
CREATE DATABASE SCOPED CREDENTIAL sqlcred
WITH
    IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = 'sv=xxx...';
GO

CREATE EXTERNAL DATA SOURCE secureFiles
WITH (
    LOCATION = 'https://mydatalake.blob.core.windows.net/data/secureFiles/'
    CREDENTIAL = sqlcred
);
GO
```

In addition to SAS authentication, have other options for defining credentials, see the Synapse Analytics documentation.

**Creating an external file format:** A data source simplifies the code for accessing files with OPENROWSET. Likewise, we can create external file formats to contain format details. For example:

```SQL
CREATE EXTERNAL FILE FORMAT CsvFormat
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS(
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"'  
        )
    );
GO
```

**Creating an external table:** If we need to do a lot of analysis or reporting with files in the data lake, the OPENROWSET function can make the code quite complicated. We can simplify access by encapsulating the files in an external table, which can then be queried with a standard SQL SELECT statement. To do this, use the CREATE EXETERNAL TABLE statement, specify the column schema, and include a WITH clause to specify the external data source, relative path, and external file format. Example:

``` SQL
CREATE EXTERNAL TABLE dbo.products
(
    product_id INT,
    product_name VARCHAR(20),
    list_price DECIMAL(5,2)
)
WITH
(
    DATA_SOURCE = files,
    LOCATION = 'products/*.csv',
    FILE_FORMAT = CsvFormat
);
GO

SELECT * FROM dbo.products;
```

By creating a database that contains the external objects discussed here, we provide a relational database layer over files in a data lake. This makes it easier for data analysts and reporting tools to access the data with standard SQL query semantics.


## 2.2. Use Azure Synapse serverless SQL pools to transform data in a data lake

While data analysts commonly use SQL to query data, data engineers often use SQL to transform data, often as part of a data ingestion pipeline or an ETL process.

### 2.2.1. Transform data files with the CREATE EXTERNAL TABLE AS SELECT statement

SQL has many features for manipulating data, for example for filetering rows and columns, renaming data, converting between data types, calculated derived data fields, manipulate strings, and grouping and aggregating data.

With serverless SQL, we can store the result of SELECT statements in a selected file format with a metadata table schema that can be queried with SQL. A CREATE EXTERNAL TABLE AS SELECT (CETAS) statement that contains a SELECT statement for querying and manipulating data from any valid data source, and the results of the query are persisted in an external table.

Using CETAS statements, we can use SQL for ETL processing of data before sending downstream for further processing or analysis. Subsequent operations on the transformed data can then be performed both against the relational table in the SQL pool database or directly against the underlying data files.

**Creating external database objects to support CETAS:** To use CETAS expressions, we need to first create an external data source and an external file format. When using a serverless SQL pool, these objects should be created in a custom database, not in the built-in database. See section 2.1.3 above for more on creating external objects.

**Using the CETAS statement:** After creating the external objects, we can use the CETAS statement to transform data and store the results in an external table.

For example, say we've created some external objects:

``` SQL
CREATE EXTERNAL DATA SOURCE files
WITH (
    LOCATION = 'https://mydatalake.blob.core.windows.net/data/files/',
    TYPE = BLOB_STORAGE,
    CREDENTIAL = storageCred
);

CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
```

Say we want to transform some source data that consists of sales orders in .csv files in a folder in a data lake. We want to filter the data to include only orders marked as 'special order', and save the results as Parquet files in a different folder in the same data lake. We can use the same external data source for the source and destination folders. So the code for accomplishing all this can look like:

``` SQL
CREATE EXTERNAL TABLE SpecialOrders
    WITH (
        LOCATION = 'special_orders/',
        DATA_SOURCE = files,
        FILE_FORMAT = ParquetFormat
    )
AS
SELECT OrderID, CustomerName, OrderTotal
FROM
    OPENROWSET(
        BULK 'sales_orders/*.csv',
        DATA_SOURCE = 'files',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS source_data
WHERE OrderType = 'Special Order';
```

Note that the relative file paths in LOCATION and BULK are relative to the file path referenced by *files*.

It's important to not that we have to use an external data source to specify where transformed data for the external table should be saved. But the source data can come from somewhere else, either from a different data source or a fully qualified path. For example, in the above code we could remove the DATA_SOURCE line in OPENROWSET and replace the string following BULK with something like `'https://mystorage.blob.core.windows.net/data/sales_orders/*.csv'`.

**Dropping external tables:** If we no longer need the external table with the transformed data, we can drop it from the database.

``` SQL
DROP EXETERNAL TABLE SpecialOrders;
```

but note that external tables are merly a metadata abstraction over the files that contains the actual data. So dropping an external table does *not* delete the underlying files.

### 2.2.2. Encapsulate data transformations in a stored procedure

It's considered good practice to encapsulate transformation operations like CETAS statements in stored procedures. It makes it easier to operationalise data transformations by enabling us to supply parameters, retrieve outputs, and include additional logic in a single procedure call.

Here's an example that creates a stored procedure that drops the external table if it already exists before recreating it with order data for the specified year:

``` SQL
CREATE PROCEDURE usp_special_orders_by_year @order_year INT
AS
BEGIN

	-- Drop the table if it already exists
	IF EXISTS (
                SELECT * FROM sys.external_tables
                WHERE name = 'SpecialOrders'
            )
        DROP EXTERNAL TABLE SpecialOrders

	-- Create external table with special orders
	-- from the specified year
	CREATE EXTERNAL TABLE SpecialOrders
		WITH (
			LOCATION = 'special_orders/',
			DATA_SOURCE = files,
			FILE_FORMAT = ParquetFormat
		)
	AS
	SELECT OrderID, CustomerName, OrderTotal
	FROM
		OPENROWSET(
			BULK 'sales_orders/*.csv',
			DATA_SOURCE = 'files',
			FORMAT = 'CSV',
			PARSER_VERSION = '2.0',
			HEADER_ROW = TRUE
		) AS source_data
	WHERE OrderType = 'Special Order'
	AND YEAR(OrderDate) = @order_year
END
```

Other benefits of stored procedures:

- Reduces client to server network traffic: the commands in a procedure are executed as a single batch of code, so we only need to send a call to execute the procedure across the network.
- Provides a security boundary: users and client programs without direct permissions to access a database can still do operations via a procedure. The procedure controls what activities are performed and protects the underlying database objects, so we don't need to manage permissions at the individual object level which simplifies the security layers.
- Eases maintenance: changes in the logic or file system locations involved in the data transformation can be applied only to the stored procedure. No need to update client applications or other calling functions.
- Imporved performance: stored procedures are compiled the first time they're executed, and the resulting execution plan is held in the cache and reused on later runs of the same procedure, resulting in it taking less time to process the procedure.

### 2.2.3. Include a data transformation stored procedure in a pipeline

We might keep a CETAS in a stored procedure to make it easier to perform data transformations repeatedly. Then in Azure Synapse Analytics and Azure Data Factory, we can create pipelines that connect to linked services, like ADLSG2 storage accounts that host data lake files, and serverless SQL pools. Thus we can call our stored procedures as part of an overall ETL pipeline.

For example, we could create a pipeline that includes:
- A **Delete** activity that deletes the target folder for the transformed data if it already exists
- A **Stored procedure** activity that connects to our serverless SQL pool and runs the stored procedure that encapsulates our CETAS operation

We can also use pipelines to schedule the operation to run at specific times or based on specific events (e.g. new files being added to the source storage location). More information about the **Stored procedure** activity in a pipeline is found in the documentation for Azure Data Factory.