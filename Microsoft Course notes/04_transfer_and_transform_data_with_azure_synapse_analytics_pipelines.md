# 4. Transfer and Transform Data with Azure Synapse Analytics Pipelines

## 4.1. Build a data pipeline in Azure Synapse Analytics

> This module is also part of the *Implement a Data Analytics Solution with Azure Synapse Analytics* learning path, see section 5.6. The notes for this module will be contained here.

Pipelines can be used to orchestrate sequences of data transfer and transformation activities and can be used to implement ETL workloads. Will focus on Azure Synapse Analytics and Synapse Studio here, but pipelines also work similarly in Azure Data Factory with comparison of the two approaches [here](https://learn.microsoft.com/en-us/azure/synapse-analytics/data-integration/concepts-data-factory-differences).

### 4.1.1. Understand pipelines in Azure Synapse Analytics

Some core concepts:
- Activities: Executeable tasks in a pipeline. We can define a flow of activities by connecting them into a sequence, and the outcome of an activity can be used to direct the flow to the next activity in the sequence. Activities can encapsulate data transfer operations as well as more complex data flows as part of an ETL operation. Activities can encapsulate processing tasks on specific systems, like running a Spark notebook or calling an Azure function. And we have *control flow* activities that can be used to implement loops, conditional branching, or manage variable and parameter values.
- Integration runtime: Provides an execution context in which the pipeline can run, used to initiate and coordinate activities in the pipeline.
- Linked services: Allows us to access external services and use them as activities in a pipeline. For example, we might want to run a notebook in Azure Databricks or call a stored procedure in Azure SQL Database. Linked services are defined at the Synapse Analytics workspace level and can be shared across multiple pipelines.
- Datasets: Defines the schema for each data object that will be used in the pipeline, has an associated linked service to connect to its source. Datasets can be inputs or outputs for activities. They are also defined on workspace level and shared across pipelines.

### 4.1.2. Create a pipeline in Azure Synapse Studio

Can do this with shortcuts on the **Home** page, but primarily it|s done in the **Integrate** page. Can use the graphical design interface, where we can drag and drop and organise activities on a visual canvas. We can select an activity and use its properties pane to configure its settings. Then we can define logical sequences of activites including connecting them with **Succeeded**, **Failed**, and **Completed** dependenct conditions.

Alternatively, we can create and edit pipelines via JSON code. An example that creates a pipeline with a **Copy Data** activity:

``` JSON
{
  "name": "CopyPipeline",
  "properties": {
    "description": "Copy data from a blob to Azure SQL table",
    "activities": [
      {
        "name": "CopyFromBlobToSQL",
        "type": "Copy",
        "inputs": [
          {
            "name": "InputDataset"
          }
        ],
        "outputs": [
          {
            "name": "OutputDataset"
          }
        ],
        "typeProperties": {
          "source": {
            "type": "BlobSource"
          },
          "sink": {
            "type": "SqlSink",
            "writeBatchSize": 10000,
            "writeBatchTimeout": "60:00:00"
          }
        },
        "policy": {
          "retry": 2,
          "timeout": "01:00:00"
        }
      }
    ]
  }
}
```

### 4.1.3. Define data flows

A Data Flow is a common activity type for defining data flow and transformation. Data flows consist of:
- Sources - the input data to be transferred
- Transformations - operations that can be applied to the data while it moves through the data flow
- Sinks - targets into which the data will be loaded

When addiding a Data Flow activity to a pipeline, we can open it in a separate interface where we can create and configure the data flow elements.

An important part of creating data flows is defining mappings for the columns as the data moves through the various stages, to ensure column names and data types are defined appropriately. During developement, we can enable the Data flow debug option to pass a subset of data through the flow, which is useful to test that the columns are mapped correctly.

### 4.1.4. Run a pipeline

After publishing a pipeline, we can use triggers to run it. Triggers for the pipeline can be that it should run immediately, at scheduled intervals, or in response to events (like new data being added to a data lake). Each individual run of a pipeline can be monitored in the **Monitor** page in Synapse Studio. Being able to monitor past and ongoing pipeline runs is useful for troubleshooting. And by integrating Synapse Analytics and Microsoft Purview, we can use pipeline run history to track data lineage data flows.