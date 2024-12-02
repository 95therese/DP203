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



## 1.2. Introduction to Azure Data Lake Storage Gen2

Traditionally, organisations have built data warehouses and BI solutions based on relational database systems. But to be able to work with unstructured data, it's become common to work with data lakes instead.

Data lakes provide file-based storage, usually in a distributed file system that supports high scalability for massive volumes of data. Can store un/semi-/structured files in the data lake and then consume them from there in big data processing technologies, like Apache Spark.

Azure Data Lake Storage Gen2 (ADLSG2) provides a cloud-based solution for data lake storage in Microsoft Azure, and underpins many large-scale analytics solutions built on Azure.

### 1.2.1. Understand Azure Data Lake Storage Gen2

Data lakes oftens store data in the form of blobs or files. ADLSG2 combines a file system with a storage platform. It builds on Azure Blob storage capabilities to optimise it specifically for analytics workloads. The integration enables analytics performance, the tiering and data lifecycle management capabilities of Blob storage, and the high-availability, security, and durability capabilities of Azure Storage.

ADLSG2 is designed to deal with this variety and volume of data at exabyte scale while also securely handling hundreds of gigabytes of throughput. So ADLSG2 can be used for both real-time and batch solutions.

**Benefits:**
- Hadoop compatible access: can treat the data as if it's stored in a Hadoop Distributed File System. So can store the date in one place and access it rhough compute technologies like Azure Databricks, Azure HDInsight, and Azure Synapse Analytics without moving the data between environments. Can also use storage mechanisms like the parquet format.

- Security: have support for access control lists (ACLs) and Portable Operating System Interface (POSIX) permissions that don't inherit the permissions of the parent directory. Can set permissions at directory level or file level. This security is configurable thorugh technologies like Hive and Spark or utilities like Azure Storage Explorer. All stored data is encrypted at rest using either Microsoft or customer-managed keys.

- Performance: data is stored into a hierarchy of directories, like a file system, for easier navigation. Hence data processing requires less computational resources.

- Data redundancy: ADLSG2 takes advantage of the Azure Blob replication models that provide data redundancy in a single data center with locally redundant storage (LRS) or to a secondary region with Geo-redundant storage (GRS).

When planning for a data lake, the data engineer need to give consideration to structure, data governance and security. For example, need to think about:
- Which type of data will be stored
- How the data will be transformed
- Who should access the data
- What the typical access patterns are

This approach helps with planning for access control governance across the lake. Data engineers also need to prevent the lake from becoming a data swamp, which can be ensured by establishing a baseline and making sure to follow best practices for ADLSG2.

### 1.2.2 Enable Azure Data Lake Storage Gen2 in Azure Storage

ADLSG2 isn't a standalone Azure service, but a configurable capability in Azure Storage.

To enable ADLSG2 through the Azure portal when creating a storage account, select the option **Enable hierarchical namespace** in the **Advanced** tab.

In an existing Azure Storage account, enable ADLSG2 by using the **Data Lake Gen2 upgrade** wizard.

### 1.2.3. Compare Azure Data Lake Store to Azure Blob Storage

Azure Blob storage stores blobs in a flat namespace within a blob container. Can organise the blobs into virtual folders, but they are stored as as a single-level hierarchy. Can access the data with HTTP or HTTPS.

ADLSG2 uses a hierachical namespace to organise blob data into directories as well as metadata about the directories and the files in them. This allows operations on the directories or the files to be done in a single operation, which can't be done with flat namespaces. Hierarchical namespaces keep the data organised, which gives better storage and retrieval performance for analytical uses and hence lowers the cost of analysis.

Recommended to disable Hierarchical Namespace if you want to store data without performing analysis and enable it if you do want to do analytics. Blob storage can also be used to archive rarely used data or store website assets like images and media. Since ADLSG2 is integrated into the Azure Storage platform, applications can use either the Blob APIs or the ADLSG2 file system APIs to access the data.

### 1.2.4. Understand the stages for processing big data

Data lakes has a role in many big data architechtures, and these architectures can involve the creation of, e.g. an enterprise data warehouse, advanced analytics against big data, or a real-time analytical solution.

But there are four stages for processing big data solutions that all architectures have in common:
- **Ingestion**: Identifying the technology and processes that are used to acquire the source data. The data can be in the form of foles, logs, and other unstructured data forms. The choice of technology will depend on the frequency of the data transfer. For example, pipelines in Azure Synapse Analytics or Azure Data Factory for batch processing, or Apache Kafka for HDInsight or Stream Analytics for real-time ingestion.

- **Store**: Identifying where to place the ingested data. ADLSG2 is obviously a candidate for big data processing.

- **Prep and train**: Identifying the technologies that are used for data preparation and model training and scoring for machine learning solutions. Common technologies include Azure Synapse Analytics, Azure Databricks, Azure HDInsight, and Azure Machine Learning.

- **Model and serve**: Identifying the technologies that will present the data to users. These include visualisation tools like Microsoft Power BI and analytical data stores like Azure Synapse Analytics. Depending on business requirements, a combination may be used.

### 1.2.5. Use Azure Data Lake Storage Gen2 in data analytics workloads

Here are examples of how ADLSG2 can be used as part of analytical workloads:

**Big data processing and analytics:** Big data scenarios are analytical workloads that involve massive *volumes* of data in a *variety* of formats that needs to be processed at a fast *velocity*, the "three v's". ADLSG2 provides a scalable and secure distributed data store on which big data services like Azure Synapse Analytics, Azure Databricks, and Azure HDInsight can apply data processing frameworks like Apache Spark, Hive, and Hadoop. We can perform tasks in parallel, resulting in high-performance and scalability even when working with massive amounts of data.

**Data warehousing:** Data warehousing is an architecture and process involving the extraction of data and transforming it into structures suitable for analytical workloads. These days it involves large amounts of data stored as files in a data lake with relational tables in a data warehouse. Often the data is staged in a data lake to facilitate distributed processing before being loaded into a relational data warehouse. Sometimes the data warehouse uses external tables to define a relational metadata layer over files in the data lake, the result being a hybrid 'data lakehouse' or 'lake database' architecture. The data warehouse can then support analytical queries for reporting and visualisation.

There are several ways to implement this kind of architecture. For example, Azure Synapse Analytics can host pipelines to do ETL processes with Azure Data Factory technology. The processes would extract the data from operational data sources and load it into a data lake in an ADLSG2 container, then the data is processed and loaded into a relational data warehouse in an Aure Synapse Analytics dedicated SQL pool, from where it can suppor data visualisation and reporting using Microsoft Power BI.

**Real-time data analytics:** Streams of data can be generated continuously from, for example, IoT devices and social media platforms. These events are often captured in a queue for real-time processing. An example of a technology that can do this is Azure Event Hubs. The captured data is then processed, often to aggregate data over temporal windows. Azure Stream Analytics allows one to create jobs that query and aggregate event data as it arrives, and write the results to an output sink. ADLSG2 can serve as such a sink.

**Data science and machine learning:** Data science involves analysing large volumes of data, often using tools like Apache Spark and Python. ADLSG2 can store the massive amounts of data required in data science workloads.

Machine learning is a subarea of data science that deals with training predictive models. This model training requires a lot of data and the ability to process the data efficiently. Azure Machine Learning can run Python code in notebooks, and the data stored in ADLSG2 containers can be processed to train models, which can then be deployed as production web services to support predictive analytical workloads.

### 1.2.6. Bonus: More on ADLSG2

The module *Introduction to Azure Data Lake Storage Gen2* is not just part of the DP 203 course, but also the learning path [*Large-Scale Data Processing with Azure Data Lake Storage Gen2*](https://learn.microsoft.com/en-gb/training/paths/data-processing-with-azure-adls/). This learning path has two additional modules that further explore ADLSG2.

The first of the additional modules, *Upload data to Azure Data Lake Storage*, consists of an exercise that demonstrate how to create an Azure Storage account with hierarchical namespace activated and an exercise introducing how to access and interact with an Azure Storage account via the Azure Storage Explorer, which can be installed locally on one's computer.

The second module, *Secure your Azure Storage account*, explores how the the data in Azure Storage accounts is kept secure. It also mentions some security features of ADLSG2 and how they are integrated into analytics services that use the data in the data lake.

## 1.3. Introduction to Azure Synapse Analytics

> This module is also part of the *Implement a Data Analytics Solution with Azure Synapse Analytics* learning path, see section 5.1. The notes for this module will be contained here.

### 1.3.1. What is Azure Synapse Analytics

Four analytical techniques that are commonly used by organisations are as follows:

- **Descriptive analytics:** 'What is happening?' This is typically answered by creating a data warehouse and store historical data for multidimensional modeling and reporting.

- **Diagnostic analytics:** 'Why is it happening?' This involves exploring existing information in the data warehouse, but also involves a wider search of one's data estate to find more data to be able to perform this analysis.

- **Predictive analysis:** 'What is likely to happen in the future based on previous trends and patterns?'

- **Prescriptive analytics:** deciding what to do based on (near) real-time analysis, uses predictive analysis.

Azure Synapse Analytics is a solution where one can perform all of these types of analyses. One can use skills from commonly used data technologies, like SQL and Apache Spark, but remaining in a centrally managed service with a single, consistent user interface.

### 1.3.2. How Azure Synapse Analytics works

Azure Synapse Analytics combines a centralised service for data storage and processing with an extensible architecture through which *linked services* allows one to integrate common data stores, processing platforms and visualisation tools.

**Creating and using an Azure Synapse Analytics workspace**: A Synapse Analytics workspace defines an instance of the Synapse Analytics service where one can manage the services and data resources needed for our analytics solution. We can create a workspace in an Azure subscription using the Azure portal, the Azure PowerShell, the Azure CLI, or with an Azure Resource Manager or Bicep template.

After creating a workspace, we interact with it using *Synapse Studio*, a web-based portal.

**Working with files in a data lake**: A workspace usually has a defautl data lake, which is implemented as a linked service to an ADLSG2 container. We can add linked services for multiple data lakes that are based on different storage platforms as required.

**Ingesting and transforming data with pipelines**: Data lakes or warehouses often contain data extracted from multiple sources. Azure Synapse Analytics has built-in support for creating, running, and managing pipelines that orchestrate the necessary activities for retrieving data from a range of sources, transforming the data as needed, and load the result into an analytical store. The pipelines are similar to the ones in Azure Data Factory, so experience with Data Factory is transferable to Synapse Analytics.

**Querying and manipulating data with SQL**: Synapse Analytics supports SQL-based data querying and manipulation through two kinds of pools based on the SQL Server engine, namely a built-in *serverless* pool optimised for using relational SQL semantics and custom *dedicated* SQL pools that host relational data warehouses.

Synapse Analytics can parallelise SQL operations, making it a highly scalable solution for relational data processing. You can use the built-in serverless pool for cost-effective analysis and processing of file data in the data lake, and use dedicatd SQL pools to create relational data warehouses.

**Processing and analysing data with Apache Spark**: Apache Spark is an open source platform for big data analytics, and it supports several languages including Python, Scala, Java, SQL, and C#. In Synapse Analytics we can create one or more Spark pools and use interactive notebooks to build solutions for data analytics, machine learning, and data visualisation.

**Exploring data with Data Explorer**: Azure Synapse Data Explorer is a data processing engine that uses Kusto Query Language (KQL) to get high performance, low-latency analysis of batch and streaming data.

**Integrating with other Azure data services**: We can integrate Synapse Analytics with other Azure data services, including:

- *Azure Synapse Link*: enables near real-time synchronisation between operational data in Azure Cosmos DB, Azure SQL Database, SQL Server, and Microsoft Power Platform Dataverse and analytical data storage that can be queried in Synapse Analytics.

- *Microsoft Power BI*: data analysts can integrate a Power BI workspace into a Synapse workspace and to interactive data visualisation in Synapse Studio.

- *Microsoft Purview*: organisations can catalogue data assets in Synapse Analytics, and makes it easier for data engineers to find data assets and track data lineage when implementing data pipelines that ingest data into Synapse Analytics.

- *Azure Machine Learning*: data analysts and scientists can integrate predictive model training and consumption into analytical solutions.

### 1.3.3. When to use Azure Synapse Analytics

Common use cases for organisations and industries are identified by the need for:

- **Large-scale data warehousing**: Data warehousing includes the need to integrate all data, including big data, to reason over data for analytics and reporting purposes from a descriptive analytics perspective, independent of its location or structure.

- **Advanced analytics**: Organisations can do predictive analysis both with the native features of Synapse Analytics and by integrating with other technologies, like Azure Machine Learning.

- **Data exploration and discovery**: Data analysts, engineers, and scientists can use the serverless SQL pool functionality to explore the data available. Can do data discovery, diagnostic analytics, and exloratory data analysis.

- **Real time analytics**: Synapse Analytics can do (near) real-time data capture, storing, and analysis with features like Synapse Link or through the integration services like Stream Analytics and Data Explorer.

- **Data integration**: Azure Synapse Pipelines allows you to ingest, prepare, model, and serve the data to be used by downstream systems. Can only be done by components of Synapse Analytics.

- **Integrated analytics**: Synapse Analytics integrates the analytics landscape into one service, making it less complicated to build a cohesive service and freeing up time to work with the data.