# 2. Build Data Analytics Solutions Using Azure Synapse Serverless SQL Pools

As prerequisites for this learning path, Microsoft says to consider completing the following learning paths:
- [Explore data analytics in Azure](https://learn.microsoft.com/en-us/training/paths/azure-data-fundamentals-explore-data-warehouse-analytics/?azure-portal=true), which is part of the DP-900 syllabus. So if you've done DP-900, you've likely already done this.
- [Get Started Querying with Transact-SQL](https://learn.microsoft.com/en-us/training/paths/get-started-querying-with-transact-sql/). Something to consider if you're new to T-SQL (or to SQL in general), or if you want some practice with T-SQL. It's a bit long, though

> Note to self: update when this section is done how necessary/useful the T-SQL path is.

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

