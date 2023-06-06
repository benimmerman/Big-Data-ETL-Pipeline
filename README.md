# DE_Bootcamp_Midterm
Midterm project from the WeCloudData Data Engineering Bootcamp

# Architecture:

![image](https://user-images.githubusercontent.com/113261578/228686403-20590f97-bda1-49b1-a3f4-da293fb2a83e.png)

# Project Summary:

The purpose of this project is to mimic a realistic ETL process that can be found in real world businesses: data files are sent to the companies data lake (S3 bucket) everyday from an internal transaction database. The company first needs to scan the S3 bucket; when the files are ready, the ETL process will start. For example, every day at 2 am, the data is dumped from the transaction database to S3 bucket, and when all the data has been dumped, the EMR cluster will run to process the data. The processed data will then be stored in a new S3 bucket for the next day's usage.

# Running This Code:

You will need to set up Airflow on an AWS EC2 instance. 
You will also need to set up an AWS EMR cluster.

--The project mimics an ETL process found in some companies. 

--CSV files are imported from a data warehouse to an S3 bucket once daily. A lambda function (lambda_func.py) checks the S3 bucket at a certain time every day using CloudWatch.

--Once the lambda function determines that the newest CSV files have been uploaded, the files are sent to Airflow which has been set up using Docker in an EC2 instance.

--The DAG (midterm_dag.py) first retrieves the files and pushes them into Airflow.

--The next DAG step connects to an EMR cluster and inserts arguments into the Spark session. Among these arguments are the Pyspark script (midterm_workflow.py) to execute, and the S3 bucket location to dump the transformed data.

--The Pyspark script uses SQL queries to create dataframes from the CSV files, then transform the data into the desired format.

--The dataframes are then converted to parquet files and are sent to the desired S3 bucket location.

--The last step Airflow takes will check if the EMR cluster executed the job successfully.

--When the parquet files are successfully uploaded to the output S3 bucket, AWS tools like Athena and Glue are used to help visualize the data in Superset or Power BI to create tables and charts for analysis.
