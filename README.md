# 🚗 Uber Pipeline Data Engineering End-to-End Project

## Objective

In this project, I designed and implemented an end-to-end data pipeline that consists of several stages:
1. Extracted data from NYC Trip Record Data website and loaded into Google Cloud Storage for further processing.
3. Transformed and modeled the data using fact and dimensional data modeling concepts using Python on Jupyter Notebook.
4. Spinned up a virtual machine using Google Compute Engine and deployed Airflow using Docker.
4. Using ETL concept, I orchestrated the data pipeline Airflow and loaded the transformed data into Google BigQuery.
5. Developed a dashboard on Looker Studio.

As this is a data engineering project, my emphasis is primarily on the engineering aspect with a lesser emphasis on analytics and dashboard development.

The sections below will explain additional details on the technologies and files utilized.

## Table of Content

- [Dataset Used](#dataset-used)
- [Technologies](technologies)
- [Data Pipeline Architecture](#data-pipeline-architecture)
- [Date Modeling](#data-modeling)
- [Step 1: Cleaning and Transformation](#step-1-cleaning-and-transformation)
- [Step 2: Storage](#step-2-storage)
- [Step 3: ETL / Orchestration](#step-3-etl--orchestration)
- [Step 4: Analytics](#step-4-analytics)
- [Step 5: Dashboard](#step-5-dashboard)

## Dataset Used

This project uses the TLC Trip Record Data which include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

More info about dataset can be found in the following links:
- Website: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
- Raw Data (CSV): https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2016-03.parquet

## Technologies

The following technologies are used to build this project:
- Language: Python, SQL
- Extraction and transformation: Jupyter Notebook, Cursor IDE, Google BigQuery
- Data Lake: [Google Cloud Storage](https://cloud.google.com/storage?hl=en)
- Virtual Machine: [Google Compute Engine](https://cloud.google.com/products/compute?hl=en)
- Containerization: [Docker](https://www.docker.com/)
- Orchestration: [Airflow](https://airflow.apache.org/)
- Data Warehouse: [Google BigQuery](https://cloud.google.com/bigquery)
- Dashboard: [Looker Studio](https://lookerstudio.google.com)

## Data Pipeline Architecture



Files in the following stages:
- Step 1: Cleaning and transformation - [Uber Data Engineering.ipynb]
- Step 2: Storage
- Step 3: ETL, Orchestration - Mage: [Extract], [Transform]
- Step 4: Analytics - [SQL script]
- Step 5: [Dashboard]

## Data Modeling

The datasets are designed using the principles of fact and dim data modeling concepts. 



## Step 1: Cleaning and Transformation

In this step, I loaded the CSV file into Jupyter Notebook and carried out data cleaning and transformation activities prior to organizing them into fact and dim tables.

Here's the specific cleaning and transformation tasks that were performed:
1. Converted `tpep_pickup_datetime` and `tpep_dropoff_datetime` columns into datetime format.
2. Removed duplicates and reset the index.

Link to the script: [https://github.com/raefaidid/uber-pipeline-project/blob/main/notebooks/playground.ipynb](https://github.com/raefaidid/uber-pipeline-project/blob/main/notebooks/playground.ipynb)



After completing the above steps, I created the following fact and dimension tables below:



## Step 2: Storage



## Step 3: ETL / Orchestration

1. Begin by launching the SSH instance and running the following commands below to install the required libraries.



```python
# Install python and pip 
sudo apt-get install update

sudo apt-get install python3-distutils

sudo apt-get install python3-apt

sudo apt-get install wget

wget https://bootstrap.pypa.io/get-pip.py

sudo python3 get-pip.py

# Install Google Cloud Library
sudo pip3 install google-cloud

sudo pip3 install google-cloud-bigquery

# Install Pandas
sudo pip3 install pandas
```



2. After that, I deployed Airflow using docker-compose following the official documentation from [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).


3. Next, I conduct orchestration in Mage by accessing the external IP address through a new tab. The link format is: `http://<external IP address>:<port number>`.

After that, I create a my pipeline as a DAG in Airflow:
- Extract, Transform, Load: https://github.com/raefaidid/uber-pipeline-project/blob/main/src/uber-yellow-taxi-pipeline.py


## Step 4: Analytics

After running the pipeline in Airflow, the fact and dim tables are generated in Google BigQuery.



Here's the additional analyses I performed:
1. Find the top 10 pickup locations based on the number of trips


2. Find the total number of trips by passenger count:


3. Find the average fare amount by hour of the day:


## Step 5: Dashboard

After completing the analysis, I loaded the relevant tables into Looker Studio and created a dashboard, which you can view here.



***
