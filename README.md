# DE_Bootcamp_Midterm
Midterm project from the WeCloudData Data Engineering Bootcamp

# Architecture:

![image](https://user-images.githubusercontent.com/113261578/228686403-20590f97-bda1-49b1-a3f4-da293fb2a83e.png)

# Project Summary:

The purpose of this project is to mimic a realistic ETL process that can be found in real world businesses: data files are sent to the companies data lake (S3 bucket) everyday from an internal transaction database. The company first needs to scan the S3 bucket; when the files are ready, the ETL process will start. For example, every day at 2 am, the data is dumped from the transaction database to S3 bucket, and when all the data has been dumped, the EMR cluster will run to process the data. The processed data will then be stored in a new S3 bucket for the next day's usage.

# Running This Code:

Create a Snowflake account and use the script load_raw_data.sql to load the transaction data. Once the data is loaded you can use snowflake_task.sql to set up automatic uploads of the newest csv files to your S3 bucket. Alternatively, you can use manually_load_data_to_S3.sql to manually upload the files to your S3 bucket.



You will need to set up Airflow on an AWS EC2 instance. Since Airflow will be running on an EC2 instance, you will need to create a new IAM role for the EC2 instance to communicate with the EMR cluster. Select the trusted entity type as AWS service, then common use cases as EC2. Add permissions: AmazonEMRFullAccessPolicy_v2. Set the role name as airflow_tirgger_emr.
After the new role is created go to your EC2 settings and attach the new role to the EC2. Select actions, then security, then modify IAM role. Select airflow_trigger_emr and select save.
After Lambda sends a signal to Ariflow, Airflow will unpack the data from Lambda as the parameter for EMR. The airflow script used in this project is midterm_dag.py. The DAG first receives the files csv files and pushesw them into Airflow. The next step in the DAG connects to your EMR cluster and inserts arguments into the Spark session to start the process of transforming the data. Once the EMR cluster is finished with this process, Airflow will check if the EMR cluster executed the job successfully and end the session.

You will also need to set up an AWS EMR cluster. Airflow will give the EMR cluster all of its PySpark arguments from the midterm_dag.py file, and the EMR will run the file midterm_workflow.py to transform the data from the csv files. Once the process is complete, new parquet files will be created with the transformed data and sent to a new S3 bucket.

When the parquet files are successfully uploaded to the output S3 bucket, AWS tools like Athena and Glue can be used to help visualize the data in Superset or Power BI to create tables and charts for analysis.
