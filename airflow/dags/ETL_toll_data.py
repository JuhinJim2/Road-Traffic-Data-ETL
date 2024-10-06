# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'J',
    'start_date': days_ago(0),
    'email': ['dummy_email.gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='ETL toll data DAG',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the first task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf "/mnt/c/Users/audre/OneDrive/Desktop/Road Data ETL/tolldata.tgz" -C "/mnt/c/Users/audre/OneDrive/Desktop/Road Traffic Data ETL/extracted_data"',
    dag=dag,
)

# define the second task
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# define the third task
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='tr "\t" "," < tollplaza-data.tsv | cut -d"," -f5,6,7 > tsv_data.csv',
    dag=dag,
)

# define the fourth task
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='awk \'$1=$1\' OFS=, payment-data.txt | cut -d"," -f10,11 > fixed_width_data.csv',
    dag=dag,
    )

# define the fifth task
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

# define the sixth task
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr \'[:lower:]\' \'[:upper:]\' < extracted_data.csv > transformed_data.csv',
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> \
extract_data_from_fixed_width >> consolidate_data >> transform_data
