# 6. Work with Data Warehouses Using Azure Synapse Analytics

## 6.1. Analyse data in a relational data warehouse

> This module is also part of the *Implement a Data Analytics Solution with Azure Synapse Analytics* learning path, see section 5.5. The notes for this module will be contained here.

### 6.1.1. Design a data warehouse schema

Data warehouses contain tables that are commonly organised in a schema that is optimised for multidimensional modeling, where numerical measures associated with events known as *facts* can be aggregated by the attributes of associated entities across multiple *dimensions*. The tables in a data warehouse schema are usually organised precicely as either *fact tables* or *dimension tables*.

**Dimension tables:** Describe business entities like products, people, places, and dates. The columns correspond to attributes of an entity, and a dimension table will have a unique key column uniquely identifying each row in the table. It's actually common to have two key columns:
- a surrogate key specific to the data warehouse that uniquely identifies each row in the dimension table in the data warehouse, often just an incrementing integer number.
- an alternate key, often a natural or business key, used to identify an instance of an entity in the transactional source system from which the entity record originated, like a product code or a customer ID.

Reasons to have two types of keys include:
- the data warehouse may contain data from multiple source systems, which risks duplicate or incompatible business keys.
- simple numeric keys often perform better in queries that join lots of tables, which is common in data warehouses.
- attributes of entities may change over time, but we want data warehouses to support historic reporting and retain records for each instance at multiple points in time. For example, a customer may change address. The address associated with the business key will likely be updated to store the current address, but surrogate keys still correspond to orders made at different points in time and will store the address that was in use at the time of each order.

It's common for a data warehouse to include a dimension table representing *time*, which enables analysts to aggregate data over temporal intervals. They lowest granularity of a time dimension depends on the data we need to analyse and can be time (e.g. down to seconds) or dates. The timespan covered by the records in the table must include the earliest and latest points in time for any associated events in a related fact table. Usually there's a record for every interval in between the appropriate grain.

**Fact tables:** Store details of observations or events like sales orders, stock balances, exchange rates, or recorded temperatures. The columns contain numeric values that can be aggregated by dimensions and key columns referencing unique keys in related dimension tables. A fact table's dimension key determine its grain.

**Data warehouse schema designs:** In data warehouses, the data is generally denormalised, in contrast to transactional databases. This is to reduce the nmber of joins required to query the data. Commonly a data warehouse is organised into a star schema where a fact table is directly related to the dimension tables. The attributes of an entity can be used to aggregate measures in fact tables over multiple hierarchical levels, e.g. find total sales revenue by country, region, city, postal code, or individual customer. The attributes for each level can be stored in the same table, but if an entity has a lot of hierarchical attribute levels or some attributes are shared by multiple dimensions (e.g. customers and stores have an address), it can make sense to apply some normalisation to the timension tables. This creates a snowflake schema.

### 6.1.2. Create data warehouse tables

To create a relational data warehouse in Azure Synapse Analytics, we need to create a dedicated SQL pool. In a Synapse Analytics workspace, we can do this in the **Manage** page in Synapse Studio. Then we can specify a few configuration settings for the dedicated SQL pool:
- a unique name
- performance level, which can range from DW100c to DW30000c and which determines the cost per hour for the pool when it's running
- whether to start with an empty pool or restore an existing database from a backup
- the collation, which determines sort order and string comparison rules for the database, can't be changed after creation

After creating the dedicated SQL pool, we control its running state in the **Manage** page, like pausing it when not in use to prevent unnecessary costs. While the pool is running, we can explore it on the **Data** page and create SQL scripts to run it.

**Considerations for creating tables:** We create tables with the T-SQL statements `CREATE TABLE` or sometimes `CREATE EXTERNAL TABLE`. Options in the statement depend on the type of table we're creating, which can include fact, dimension, or staging tables. The latter are often used in the data warehousing loading process to ingest data from source systems.

When designing a star schema model for small or medium sized datasets we can use a preferred database, like Azure SQL. For larger data sets we can benefit from implementing the data warehouse in Synapse Analytics. Some important differences when creating tables in Synapse Analytics:
- Data integrity constraints: Dedicated SQL pools don't support foreign keys and unique constraints, so jobs that load data must maintain uniqueness and referential integriy for keys without relying on the table definitions in the database to do so.
- Indexes: Dedicated SQL pools support clustered indexes, but the default type is clustered columnstore. This index type gives performance advantage when querying a lot of data in a typical data warehouse schema and should be used where possible. But some tables can have datatypes that can't be included in a clustered columnstore, in which case a clustered index can be used instead.
- Distribution: Dedicated SQL pools use a massively parallel processing (MPP) architecture, instead of symmetric multiprocessing (SMP) which is used in most OLTP systems. In an MPP system, the data in a table is distributed across a pool of nodes for processing. The distribuyion types offered by Synapse Analytics are hash (a hash value is calculated for the column and used to assign the row to a compute node), round-robin (rows are evenly distributed across all compute nodes), and replicated (a copy of the table is stored on each compute node). The table type often determines which option to use, where small dimension tables use replicated distribution, large dimension tables and fact tables use hash, and staging tables use round-robin.

**Creating dimension tables:** The table definition should include surroge and alternate keys as well as columns for the attributes of the dimension we want to use. It's easiest to use an `IDENTITY` column to auto-generate an incrementing surrogate key. Example:

``` SQL
CREATE TABLE dbo.DimCustomer
(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
```

If we want to, we can create a specific schema as a namespace for the tables. Here, the default **dbo** schema is used. If we want to use a snowflake schema, then they key for the parent dimension should be included in the definition of the child dimension table. For example, if we store the geographical address in a separate table, the code might look like this:

``` SQL
CREATE TABLE dbo.DimGeography
(
    GeographyKey INT IDENTITY NOT NULL,
    GeographyAlternateKey NVARCHAR(10) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dbo.DimCustomer
(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    GeographyKey INT NULL, -- HERE!
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
```

The code for creating a time dimension table can look like this:

``` SQL
CREATE TABLE dbo.DimDate
( 
    DateKey INT NOT NULL,
    DateAltKey DATETIME NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName NVARCHAR(15) NOT NULL,
    MonthOfYear INT NOT NULL,
    MonthName NVARCHAR(15) NOT NULL,
    CalendarQuarter INT  NOT NULL,
    CalendarYear INT NOT NULL,
    FiscalQuarter INT NOT NULL,
    FiscalYear INT NOT NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
```

A common pattern for dates in a dimension table is to use the DDMMYYYY or YYYYMMDD formats as numeric surrogate key, and the date as a `DATE` or `DATETIME` datatype as the alternate key.

**Creating fact tables:** Fact tables include the keys for each dimension to which they're related, and the attributes and numeric measures for specific events or observations that we want to analyse. Example:

``` SQL
CREATE TABLE dbo.FactSales
(
    OrderDateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    StoreKey INT NOT NULL,
    OrderNumber NVARCHAR(10) NOT NULL,
    OrderLineItem INT NOT NULL,
    OrderQuantity SMALLINT NOT NULL,
    UnitPrice DECIMAL NOT NULL,
    Discount DECIMAL NOT NULL,
    Tax DECIMAL NOT NULL,
    SalesAmount DECIMAL NOT NULL
)
WITH
(
    DISTRIBUTION = HASH(OrderNumber),
    CLUSTERED COLUMNSTORE INDEX
);
```

**Creating stagin tables:** As staging tables are used as temporary storage for data entering the data warehouse, a common pattern is to structure the table to make it as efficient as possible to ingest data from its external source (often files in a data lake) into the relational database, then use SQL statements to load the data from the staging tables into the dimension and fact tables. Example:

``` SQL
CREATE TABLE dbo.StageProduct
(
    ProductID NVARCHAR(10) NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    ProductCategory NVARCHAR(200) NOT NULL,
    Color NVARCHAR(10),
    Size NVARCHAR(10),
    ListPrice DECIMAL NOT NULL,
    Discontinued BIT NOT NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);
```

If the data to be loaded is stored in files with an appropriate structure, it can be more effective to read the data directly from the source files using external tables that reference the file location. Example with Parquet files in a data lake:

``` SQL
-- External data source links to data lake location
CREATE EXTERNAL DATA SOURCE StagedFiles
WITH (
    LOCATION = 'https://mydatalake.blob.core.windows.net/data/stagedfiles/'
);
GO

-- External format specifies file format
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

-- External table references files in external data source
CREATE EXTERNAL TABLE dbo.ExternalStageProduct
(
    ProductID NVARCHAR(10) NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    ProductCategory NVARCHAR(200) NOT NULL,
    Color NVARCHAR(10),
    Size NVARCHAR(10),
    ListPrice DECIMAL NOT NULL,
    Discontinued BIT NOT NULL
)
WITH
(
    DATA_SOURCE = StagedFiles,
    LOCATION = 'products/*.parquet',
    FILE_FORMAT = ParquetFormat
);
GO
```

### 6.1.3. Load data warehouse tables

Loading a data warehouse is typically done by adding new data from files in a data lake into tables, which the `COPY` statement can do effectively:

``` SQL
COPY INTO dbo.StageProducts
    (ProductID, ProductName, ProductCategory, Color, Size, ListPrice, Discontinued)
FROM 'https://mydatalake.blob.core.windows.net/data/stagedfiles/products/*.parquet'
WITH
(
    FILE_TYPE = 'PARQUET',
    MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF'
);
```

**Considerations for designing a data warehouse load process:** A common pattern for loading a data warehouse is to stransfer data from source systems into files in a data lake, ingest the file data into staging tables, then use SQL statements to load the data into the dimension and fact tables. The data loading is usually performed as a periodic batch process, where inserts and updates to the data warehouse are coordinated to occur at a regular interval.

In most cases, the load process should be implemented to perform tasks in the following order:
1. Ingest new data into a data lake, applying pre-load clearnins or transformation as needed.
2. Load the data from files into staging tables in the relational data warehouse.
3. Load the dimension tables from the dimension data in the staging tables, updating existing rows or inserting new rows and generating surrogate key values as needed
4. Load the fact tables from the fact data in the staging tables, looking up appropriate surrogate keys for related dimensions as needed.
5. Perform post-load optimisation by updating indexes and table distribution statistics.

After loading data into staging tables, we can use a combination of `INSERT`, `UPDATE`, `MERGE`, and `CREATE TABLE AS SELECT` statements to load the staged data into dimension and fact tables.

### 6.1.4. Querying a data warehouse

After the dimension and fact tables are loaded, we can query and analyse the data with SQL. In a Synapse dedicated SQL pool we use T-SQL.

**Aggregating measures by dimension attributes:** Because of the star of snowflake schemas that are often used, aggregation queries rely on the usage of `JOIN` and `GROUP BY` clauses.

``` SQL
SELECT  dates.CalendarYear,
        dates.CalendarQuarter,
        SUM(sales.SalesAmount) AS TotalSales
FROM dbo.FactSales AS sales
JOIN dbo.DimDate AS dates ON sales.OrderDateKey = dates.DateKey
GROUP BY dates.CalendarYear, dates.CalendarQuarter
ORDER BY dates.CalendarYear, dates.CalendarQuarter;
```

We can join as many dimension tables as needed, just include more `JOIN` clauses.

With snowflake schemas, dimensions may be partially normalised. This requires multiple joins in order to relate fact tables to snowflake dimensions.

``` SQL
SELECT  cat.ProductCategory,
        SUM(sales.OrderQuantity) AS ItemsSold
FROM dbo.FactSales AS sales
JOIN dbo.DimProduct AS prod ON sales.ProductKey = prod.ProductKey
JOIN dbo.DimCategory AS cat ON prod.CategoryKey = cat.CategoryKey
GROUP BY cat.ProductCategory
ORDER BY cat.ProductCategory;
```

Note here that we need to include a join with **DimProduct** as it connects the tables of interest, but we don't return any fields from the **DimProduct** itself.

**Using ranking functions:** Another common analytical query is to partition the results based on a dimension attribute and then rank the results within each partition, e.g. rank stores within each year by their sales revenue. A few T-SQL functions that can help with this include:
- `ROW_NUMBER`: returns the orinal position of the row within the partition.
- `RANK`: returns the ranked position of each row in the ordered results. Rows of equal ranking values will have that same rank, but subsequent entries are given rank that reflect the number of higher ranking rows including ties.
- `DENSE_RANK`: same, but rows following ties are given rank positions that ignoring ties.
- `NTILE`: returns that specified percentile in which the row is located. E.g. `NTILE(4)` returnsa row's quartile.

``` SQL
SELECT  ProductCategory,
        ProductName,
        ListPrice,
        ROW_NUMBER() OVER
            (PARTITION BY ProductCategory ORDER BY ListPrice DESC) AS RowNumber,
        RANK() OVER
            (PARTITION BY ProductCategory ORDER BY ListPrice DESC) AS Rank,
        DENSE_RANK() OVER
            (PARTITION BY ProductCategory ORDER BY ListPrice DESC) AS DenseRank,
        NTILE(4) OVER
            (PARTITION BY ProductCategory ORDER BY ListPrice DESC) AS Quartile
FROM dbo.DimProduct
ORDER BY ProductCategory;
```

**Retrieving an approximate count:** It's common to start data analysis with some data exploration, for example using `COUNT`. But because of the volume of data in a data warehouse, even simple queries like counting the number of records meeting some criteria can take a while to run. Since we often don't need a precice count in these situations, we can use a function like `APPOX_COUNT_DISTINCT`:

``` SQL
SELECT dates.CalendarYear AS CalendarYear,
    APPROX_COUNT_DISTINCT(sales.OrderNumber) AS ApproxOrders
FROM FactSales AS sales
JOIN DimDate AS dates ON sales.OrderDateKey = dates.DateKey
GROUP BY dates.CalendarYear
ORDER BY CalendarYear;
```

This function uses a HyperLogLog algorithm to get an approximate count, where the result has a maximum error rate of 2% with 97% probability. The counts will be less accurate, but often sufficient for the questions we're asking at this stage. As the function finishes running much more quickly, this trade-off is often acceptable during basic data exploration.