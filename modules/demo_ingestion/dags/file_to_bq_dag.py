import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage
from typing import List

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

@dag(
    dag_id='file_to_bq_processor',
    default_args=default_args,
    description='Process files from landing bucket to BigQuery',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def file_processing_dag():
    
    @task
    def list_files() -> List[str]:
        """List files in the landing bucket and return them."""
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(LANDING_BUCKET)
        
        files_to_process = []
        for blob in blobs:
            if blob.name.endswith('.csv') or blob.name.endswith('.json'):
                files_to_process.append(blob.name)
        
        return files_to_process
    
    @task
    def load_file_to_bq(file_path: str):
        """Load a file to BigQuery."""
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
        
        # Use GCSToBigQueryOperator directly
        load_operator = GCSToBigQueryOperator(
            task_id=f'load_to_bq_{file_path}',
            bucket=LANDING_BUCKET,
            source_objects=[file_path],
            destination_project_dataset_table=f"{BQ_DATASET}.{table_name}",
            source_format=source_format,
            skip_leading_rows=skip_leading_rows,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_NEVER',
            autodetect=True,
        )
        
        # Execute the operator
        load_operator.execute(context={})
        
        return file_path
    
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def move_to_archive(file_path: str):
        """Move file to archive bucket."""
        gcs_hook = GCSHook()
        
        # Copy file to archive bucket
        gcs_hook.copy(
            source_bucket=LANDING_BUCKET,
            source_object=file_path,
            destination_bucket=ARCHIVE_BUCKET,
            destination_object=file_path
        )
        
        # Delete file from landing bucket
        gcs_hook.delete(
            bucket_name=LANDING_BUCKET,
            object_name=file_path
        )
        
        return f"File {file_path} moved to archive"
    
    @task(trigger_rule=TriggerRule.ALL_FAILED)
    def move_to_error(file_path: str):
        """Move file to error bucket."""
        gcs_hook = GCSHook()
        
        # Copy file to error bucket
        gcs_hook.copy(
            source_bucket=LANDING_BUCKET,
            source_object=file_path,
            destination_bucket=ERROR_BUCKET,
            destination_object=file_path
        )
        
        # Delete file from landing bucket
        gcs_hook.delete(
            bucket_name=LANDING_BUCKET,
            object_name=file_path
        )
        
        return f"File {file_path} moved to error"
    
    @task
    def process_files(file_list: List[str]):
        """Process each file in the list."""
        for file_path in file_list:
            # Create a processing pipeline for each file
            loaded_file = load_file_to_bq(file_path)
            move_to_archive(loaded_file)
            move_to_error(loaded_file)
        
        return "All files processed"
    
    # Define the workflow
    file_list = list_files()
    process_files(file_list)

# Create the DAG
file_processing_dag_instance = file_processing_dag()