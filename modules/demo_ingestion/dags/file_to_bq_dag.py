import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage, bigquery
from typing import List, Dict

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
    def process_all_files(file_list: List[str]) -> List[Dict[str, str]]:
        """Process all files and return results."""
        from airflow.models import TaskInstance
        from airflow.utils.context import Context
        
        # Get the DAG run ID from the context
        ti = TaskInstance(task=None, execution_date=None)
        context = Context(ti=ti)
        dag_run_id = context["dag_run"].run_id
        
        results = []
        for file_path in file_list:
            # Process each file and collect results
            try:
                # Extract file information
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
                
                # Use BigQuery client directly
                client = bigquery.Client()
                job_config = bigquery.LoadJobConfig(
                    source_format=source_format,
                    skip_leading_rows=skip_leading_rows,
                    autodetect=True,
                    write_disposition='WRITE_APPEND',
                    create_disposition='CREATE_NEVER',
                )
                
                uri = f"gs://{LANDING_BUCKET}/{file_path}"
                table_id = f"{client.project}.{BQ_DATASET}.{table_name}"
                
                # Load data to BigQuery
                load_job = client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )
                
                # Wait for the job to complete
                load_job.result()
                
                # Update the ingestion_time and ingestion_id for newly added records
                update_query = f"""
                UPDATE `{table_id}`
                SET 
                    ingestion_time = CURRENT_TIMESTAMP(),
                    ingestion_id = '{dag_run_id}'
                WHERE 
                    ingestion_time IS NULL
                    AND ingestion_id IS NULL
                """
                
                # Execute the update query
                update_job = client.query(update_query)
                update_job.result()  # Wait for the update to complete
                
                # Move file to archive bucket
                gcs_hook = GCSHook()
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
                
                results.append({
                    "status": "success", 
                    "file": file_path, 
                    "message": f"File {file_path} processed successfully",
                    "dag_run_id": dag_run_id
                })
                
            except Exception as e:
                # On error, move file to error bucket
                try:
                    gcs_hook = GCSHook()
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
                except Exception as move_error:
                    results.append({
                        "status": "error", 
                        "file": file_path, 
                        "message": f"Error processing file and moving to error bucket: {str(e)}, Move error: {str(move_error)}",
                        "dag_run_id": dag_run_id
                    })
                    continue
                
                results.append({
                    "status": "error", 
                    "file": file_path, 
                    "message": f"Error processing file: {str(e)}",
                    "dag_run_id": dag_run_id
                })
        
        return results
    
    @task
    def summarize_results(results: List[Dict[str, str]]) -> Dict[str, int]:
        """Summarize processing results."""
        success_count = sum(1 for r in results if r["status"] == "success")
        error_count = sum(1 for r in results if r["status"] == "error")
        
        # Get the DAG run ID from the first result (they all have the same run ID)
        dag_run_id = results[0]["dag_run_id"] if results else "unknown"
        
        return {
            "total_files": len(results),
            "success_count": success_count,
            "error_count": error_count,
            "dag_run_id": dag_run_id
        }
    
    # Define the workflow
    file_list = list_files()
    results = process_all_files(file_list)
    summary = summarize_results(results)

# Create the DAG
file_processing_dag_instance = file_processing_dag()