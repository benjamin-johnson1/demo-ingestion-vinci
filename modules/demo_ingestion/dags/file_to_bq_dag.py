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
    def process_single_file(file_path: str) -> Dict[str, str]:
        """Process a single file with error handling."""
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
            
            # Use BigQuery client directly instead of operator
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig(
                source_format=source_format,
                skip_leading_rows=skip_leading_rows if source_format == 'CSV' else 0,
                autodetect=True,
                write_disposition='WRITE_TRUNCATE',
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
            
            return {"status": "success", "file": file_path, "message": f"File {file_path} processed successfully"}
            
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
                return {"status": "error", "file": file_path, "message": f"Error processing file and moving to error bucket: {str(e)}, Move error: {str(move_error)}"}
            
            return {"status": "error", "file": file_path, "message": f"Error processing file: {str(e)}"}
    
    @task
    def process_all_files(file_list: List[str]) -> List[Dict[str, str]]:
        """Process all files and return results."""
        results = []
        for file_path in file_list:
            # Process each file and collect results
            # Note: We're not calling other @task functions here
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
                    skip_leading_rows=skip_leading_rows if source_format == 'CSV' else 0,
                    autodetect=True,
                    write_disposition='WRITE_TRUNCATE',
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
                
                results.append({"status": "success", "file": file_path, "message": f"File {file_path} processed successfully"})
                
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
                    results.append({"status": "error", "file": file_path, "message": f"Error processing file and moving to error bucket: {str(e)}, Move error: {str(move_error)}"})
                    continue
                
                results.append({"status": "error", "file": file_path, "message": f"Error processing file: {str(e)}"})
        
        return results
    
    @task
    def summarize_results(results: List[Dict[str, str]]) -> Dict[str, int]:
        """Summarize processing results."""
        success_count = sum(1 for r in results if r["status"] == "success")
        error_count = sum(1 for r in results if r["status"] == "error")
        
        return {
            "total_files": len(results),
            "success_count": success_count,
            "error_count": error_count
        }
    
    # Define the workflow - this is the correct way to use TaskFlow API
    file_list = list_files()
    results = process_all_files(file_list)
    summary = summarize_results(results)

# Create the DAG
file_processing_dag_instance = file_processing_dag()