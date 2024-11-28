# 1. Get Started with Data Engineering on Azure

## 1.1. Introduction to data engineering on Azure

Data engineers are reponsible for integrating, transforming, and consolidating data from various un/structured data systems into structures that are suitable for building analytics solutions. They also help to ensure that data pipelines and data stores are high-performing, efficient, organised, and reliable, given some requirements and constraints from the business.

### 1.1.1. What is data engineering

The data engineer will work with multiple types of data using many scripting or coding languages. They work with the three primary types of data: structured, semi-structured, and unstructured.

**Main tasks of a data engineer:**
- *Data integration*: Establishing links between data sources and operational or analytical services. E.g. if a business uses data that is spread across multiple systems, then it's a data engineer's job to establish links so that all the necessary data can be exctraced from all the systems.

- *Data transformation*: Transforming operational data into structures and formats that are suitable for analysis, often as part of an ETL or (increasinly) ELT process.

- *Data consolidation*: Combining extracted data from multiple sources into a consistent structure, usually to support analytics and reporting.

Many tools and scripting languages are useful for data engineers, in particular SQL and Python, but potentially also others, for example R, Java, Scala, .NET.


### 1.1.2. Important data engineering concepts

**Operational and analytical data:** Transactional data generated and stored by applications, often in (non-)relational databes, vs historical data optimised for analysis and reporting, often in a data warehouse.

One of the responsibilities of a data engineer is managing solutions that integrate both operational and analytical data sources or managing ETL solutions.

**Streaming data:** Data that is generated in real-time and often relating to specific events. Common sources of streaming data include IoT devices and social media feeds.

Data engineers often implement solutions that capture real-time streams of data and ingestthem into analytical data systems. Often the real-time data is combined with other batch processed application data.

**Data pipelines:** Data pipelines are used to orchestrate activities that transfer and transform data, and they are the primary way in which ETL solutions are implemented. They can be triggered based on a schedule or in response to events.

**Data lakes:** Storage repository that holds a lot of data in native, raw formats. Data lake stores are optimised for scaling to massive volumes (tera- or petabytes) of data. The data typically comes from several different sources and can be un/semi-/structured.

The idea of data lakes is to store everything in its original form, as opposed to transforming and processing the data before storage like in a data warehouse.

**Data warehouse:** Centralised repository of integrated data. Current and historical data is stored in relational tables organised into schemas that optimise performance of analytical queries.

Data engineers are responsible for designing and implementing relational data warehouses, and managing regular data loads into tables.

**Apache spark:** A parallel processing framework that takes advantage of in-memory processing and a distributed file storage. Common open-source software tool for dig data scenarios.

Data engineers work with Spark, using notebooks and other code artifacts to process data in a data lake and prepare it for mdoeling and analysis.


### 1.1.3. Data engineering in Microsoft Azure

Operational data can be stored in Azure services like Azure SQL Database, Azure Cosmos DB, and Microsoft Dataverse. Streaming data is captured in event broker services such as Azure Event Hubs.

The core area of responsibility for a data engineer is to capture, ingest, and consolidate the operational data into analytical stores, from where it can be modeled and visualised in reports and dashboards. The core Azure technologies for implementing data engineering workloads include:
- Azure Synapse Analytics
- Azure Data Lake Storage Gen2
- Azure Stream Analytics
- Azure Data Factory
- Azure Databricks

The analytical data stores populated with data from the data engineering workloads support data modeling and visualisation for reporting and analysis, often using tools like Microsoft Power BI.



