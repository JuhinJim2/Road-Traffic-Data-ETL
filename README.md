# Utilizing Shell and Airflow in a Dockerized Environment | Road Traffic ETL Data Pipelines

## Introudction 

This data engineering project aims to perform ETL operations on various file formats using Apache Airflow in a containerized environment. By utilizing the BashOperator, we will create data pipelines for consolidating data from multiple sources. The goal is to standardize and integrate this data for easier analysis, ensuring efficient processing and reliable delivery to support informed decision-making.

## Architecture

<img src="\Visuals\Screenshot 2024-10-13 095806.png">

## Technology Used

1. Python
2. Shell
3. Airflow
4. Docker
5. Git

# Data Pipeline

1. Extract data from a csv file
2. Extract data from a tsv file
3. Extract data from a fixed-width file
4. Transform the data
5. oad the transformed data into the staging area