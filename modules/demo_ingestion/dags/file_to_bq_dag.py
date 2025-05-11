import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage

# Get environment variables
LANDING_BUCKET = os.environ.get('LANDING_BUCKET')
ERROR_BUCKET = os.environ.get('ERROR_BUCKET')
ARCHIVE_BUCKET = os.environ.get('ARCHIVE_BUCKET')
BQ_DATASET = os.environ.get('BQ_DATASET')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def list_files_in_bucket(**kwargs):
    """List files in the landing bucket and pass them to the next task."""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(LANDING_BUCKET)
    
    files_to_process = []
    for blob in blobs:
        if blob.name.endswith('.csv') or blob.name.endswith('.json'):
            files_to_process.append(blob.name)
    
    return files_to_process

def move_file(**kwargs):
    """Move file to archive or error bucket based on task success."""
    file_path = kwargs['file_path']
    target_bucket = kwargs['target_bucket']
    
    gcs_hook = GCSHook()
    
    # Copy file to target bucket
    source_object = file_path
    target_object = file_path
    
    gcs_hook.copy(
        source_bucket=LANDING_BUCKET,
        source_object=source_object,
        destination_bucket=target_bucket,
        destination_object=target_object
    )
    
    # Delete file from landing bucket
    gcs_hook.delete(
        bucket_name=LANDING_BUCKET,
        object_name=source_object
    )

with DAG(
    'file_to_bq_processor',
    default_args=default_args,
    description='Process files from landing bucket to BigQuery',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files_in_bucket,
        provide_context=True,
    )
    
    # This is a placeholder for dynamic task generation
    # In a production environment, you would implement a proper dynamic task creation mechanism
    # For demonstration, I'll show the pattern for processing a single file
    
    def process_file(file_path):
        file_name = os.path.basename(file_path)
        table_name = f"t_raw_{os.path.splitext(file_name)[0]}"
        file_extension = file_path.split('.')[-1].lower()
        
        # Configure load job based on file type
        if file_extension == 'csv':
            source_format = 'CSV'
            skip_leading_rows = 1
        else:  # json
            source_format = 'NEWLINE_DELIMITED_JSON'
            skip_leading_rows = 0
        
        # Task to load data to BigQuery
        load_to_bq = GCSToBigQueryOperator(
            task_id=f'load_to_bq_{file_path}',
            bucket=LANDING_BUCKET,
            source_objects=[file_path],
            destination_project_dataset_table=f"{BQ_DATASET}.{table_name}",
            source_format=source_format,
            skip_leading_rows=skip_leading_rows,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_NEVER',  # Table already exists from Terraform
            autodetect=True,  # Use schema from existing table
        )
        
        # Task to move file to archive on success
        move_to_archive = PythonOperator(
            task_id=f'move_to_archive_{file_path}',
            python_callable=move_file,
            op_kwargs={
                'file_path': file_path,
                'target_bucket': ARCHIVE_BUCKET
            },
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
        
        # Task to move file to error on failure
        move_to_error = PythonOperator(
            task_id=f'move_to_error_{file_path}',
            python_callable=move_file,
            op_kwargs={
                'file_path': file_path,
                'target_bucket': ERROR_BUCKET
            },
            provide_context=True,
            trigger_rule=TriggerRule.ALL_FAILED,
        )
        
        # Set up task dependencies
        list_files_task >> load_to_bq
        load_to_bq >> move_to_archive
        load_to_bq >> move_to_error
    
    # For demonstration, process a sample file
    # In a real implementation, you would dynamically create tasks for each file
    process_file("example.csv")
    
    # Here's how you might implement dynamic task creation in a production environment
    # This code would replace the process_file("example.csv") line above
    
    """
    # This is pseudocode for dynamic task creation
    file_list = "{{ ti.xcom_pull(task_ids='list_files') }}"
    
    # In a TaskFlow API DAG or with a custom plugin, you could do:
    for file_path in file_list:
        process_file(file_path)
    """