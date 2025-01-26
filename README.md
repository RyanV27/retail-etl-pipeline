# ETL Workflow on Retail Transactions Data


## Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline using PySpark to analyze transaction data for Apple products. The workflows focus on identifying specific customer purchasing behaviors, such as:

1. Customers who bought AirPods immediately after purchasing an iPhone.
2. Customers who bought only iPhones and AirPods, with no other products.

The project uses a modular design, splitting the pipeline into separate components for extraction, transformation, and loading. Each component is designed for scalability and reusability.


## Project Structure
1. apple_analysis_workflow.ipynb       (Main script defining workflows and their execution)
2. extractor.ipynb                    (Extractors to read data from various sources)
3. transformer.ipynb                  (Transformers for data processing)
4. loader.ipynb                       (Loaders for data storage)
5. reader_factory.ipynb               (Factory methods for reading data from multiple sources)
6. loader_factory.ipynb               (Factory methods for writing data to multiple destinations)
7. customer_delta_table_loader.ipynb  (Adds the customers table to Databricks as a Delta table)
8. Transaction_Updated.csv            (Input transaction data (stored in DBFS in this example)


## Features
1. Workflows

Two workflows are defined in apple_analysis_workflow.ipynb:
* AirpodsAfteriPhoneWorkflow: Identifies customers who bought AirPods immediately after buying an iPhone.
* OnlyAirpodsAndiPhoneWorkflow: Identifies customers who bought only AirPods and iPhones.

2. Factory Design Pattern
* Reader Factory: Dynamically loads data from CSV, Parquet, or Delta Table formats.
* Loader Factory: Dynamically writes data to DBFS or Delta Tables, with options for partitioning.

3. ETL Pipeline

Each workflow is implemented using a 3-step ETL process:
* Extract: Reads transaction and customer data.
* Transform: Applies business logic to process the data.
* Load: Stores the results in Delta tables and/or Parquet files.

## Steps to Set Up
1. Clone the repository or copy the notebooks into your Databricks workspace.
2. Upload the input data (`Transaction_Updated.csv`) to DBFS under `dbfs:/FileStore/tables/Transaction_Updated.csv`.
3. Upload the input data (`Customer_Updated.csv`) to DBFS under `dbfs:/FileStore/tables/Customer_Updated.csv`.
4. Create a delta table for Customers by running the `customer_delta_table_loader.ipynb` file.
5. Ensure the following directories exist in DBFS for output storage:
   `dbfs:/FileStore/tables/apple_analysis/output/airpodsAfteriPhone`
   `dbfs:/FileStore/tables/apple_analysis/output/airpodsOnlyiPhone`


## Running a Workflow
1. Open the apple_analysis_workflow.ipynb notebook.
2. Set the workflow name as either "OnlyAirpodsAndiPhoneWorkflow" or "AirpodsAfteriPhoneWorkflow":  
  ```
  name = "OnlyAirpodsAndiPhoneWorkflow"
  ```
  ```
  name = "AirpodsAfteriPhoneWorkflow"
  ```
3. Execute the workflow using:
  ```
  WorkflowRunner(name).runner()
  ```
4. Check the output in DBFS or the Delta table.
