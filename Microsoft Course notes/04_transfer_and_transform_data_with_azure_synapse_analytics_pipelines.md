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


## 4.2. Use Spark Notebooks in an Azure Synapse Pipeline

Pipelines in Synapse Analytics are good for orchestrating activities with the data and build data integration solutions across multiple systems. For analytical data in a data lake, Apache Spark is good for processing huge volumes of data efficiently. And with the **Synapse Notebook** activity, we can run Spark notebooks as a task in a pipeline, which makes it possible to automate big data processing and integrate it into ETL workloads.

### 4.2.1. Understand Synapse Notebooks and Pipelines

Apache Spark pools in a Synapse Analytics workspace is an external resource that pipelines in Synapse can access, hence allowing us to run notebooks as part of pipelines.

Commonl with big data analytics solutions, data engineers will use Spark notebooks for inital data exploration and interactive experimentation when designing data transformation processes. When the transformation logic is done, we can do some final code optimisation and refactoring for maintainability, and then include the notebook in a pipeline. Then the pipeline can be run on a schedule or in response to an event.

The notebook is run on a Spark pool, which can be configured with the appropriate compute resources and Spark runtime for our specific workload. The pipeline itself is run in an integraiton runtime that orchestrates the activities in the pipeline, coordinating the external services needed to run them.

Some best practices for making Spark notebooks efficient and effective include:
- Keep the code organised: use clear and descriptive variable and function names, organaise the code into small, reusable chunks.
- Cache intermediate results: Spark can do this, and this can speed up performance.
- Avoid unnecessary computations: for example, if we only need a subset of the data, filter it out before running computations
- Avoid using collect() unless necessary: with large datasets, it's often better to do operations on the entire dataset rather than bringing the data into the driver node with collect().
- Use Spark UI for monitoring and debugging: Spark's web-based UI has detailed information about performance of Spark jobs, including task execution times, input and output data sizes, and more.
- Keep your dependencies version-consistent and updated: important to keep dependencies version-consistent across our cluster and to use the latest version of Spark and other dependencies if possible.

### 4.2.2. Use a Synapse notebook activity in a pipeline

To run a Spark notebook in a pipeline, we have to add a notebook activity and configure it appropriately. The **Notebook** activity is in the **Synapse** section of the activities pane in the pipeline designer. Notebooks can also be added to a pipeline from within the notebook editor.

To configure the notebook activity, edit the settings in the properties pane beneath the pipeline designer canvas. Settings include:
- Notebook: pick the notebook to run, either an existing one or create a new one.
- Spark pool: which Apache Spark pool the notebook should run on.
- Executor size: node size for the worker nodes in the pool, which determines the number of processor cores and the amount of memory allocated to worker nodes.
- Dynamically allocate executors: enables the pool to automatically scale up and down to support the workload.
- Min executors: minimum number of executors to be allocated.
- Max executors: maximum number of executors to be allocated.
- Driver size: node size for the driver node.

### 4.2.3. Use Parameters in a notebook

With parameters we can dynamically pass values for variables in the notebook each time it's run. This gives flexibility and enables us to adjust the logic in the notebook for each run. We work with parameters as follows:

**Create a *parameters* cell in the notebook:** We can declare and initialise variables in a notebook cell, then configure the cell as a **Parameters** cell with the toggle option in the notebook editor interface. Initialisation ensures that a variable has a default value, which is used if the parameter isn't set in the notebook activity.

**Set *base parameters* for the notebook activity:** After defining a parameters cell in the notebook, we can set values to be used when the notebook is run by a notebook activity in a pipeline. We do this in the **Base parameters** section of the settings for the activity. We can assign explicit parameters values or use an expression to assign a dynamic value. For example, `@pipeline().RunId` returns the unique identifier for the current runt of the pipeline.