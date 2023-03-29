import boto3
import time
import subprocess
import json
from send_email import send_email

s3_file_list = []

s3_client=boto3.client('s3')
for object in s3_client.list_objects_v2(Bucket='s3://wcd-midterm-bimmerman/snowflake_db_input/')['Contents']:
    s3_file_list.append(object['Key'])

datestr = time.strftime("%Y-%m-%d")

required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv']

# scan S3 bucket
if s3_file_list==required_file_list:
    s3_file_url = ['s3://wcd-midterm-bimmerman/snowflake_db_input/' + a for a in s3_file_list]
    table_name = [a[:-15] for a in s3_file_list]   

    data = json.dumps({'conf':{a:b for a in table_name for b in s3_file_url}})
# send signal to Airflow    
    endpoint= 'http://<airflow_url_docker_ec2>/api/experimental/dags/midterm_dag/dag_runs'

    subprocess.run(['curl', '-X', 'POST', endpoint, '--insecure', '--data', data])
    print('File are send to Airflow')
else:
    send_email()