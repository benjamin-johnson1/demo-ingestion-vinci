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
BQ_RAW_DATASET = os.environ.get('BQ_RAW_DATASET')
AUDIT_TABLE = 'bj-demo-ingestion-vinci.d_vinci_audit_eu_demo.t_audit_ingestion'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': "benjamin99.johnson@gmail.com" #use groups within an organization
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

        # Add this line to log the files found
        print(f"Files found for processing: {files_to_process}") 
        return files_to_process
    
    @task
    def process_all_files(file_list: List[str], **kwargs) -> List[Dict[str, str]]:
        """Process all files and return results."""
        # Retrieve the DAG run ID from the Airflow context
        context = kwargs
        dag_run_id = context.get("dag_run").run_id  # Safely get the DAG run ID
        
        results = []
        for file_path in file_list:
            # Process each file and collect results
            try:
                # Extract file information
                file_name = os.path.basename(file_path)
                table_name = f"t_raw_{os.path.splitext(file_name)[0]}"
                file_extension = file_path.split('.')[-1].lower()
                client = bigquery.Client()

                # Configure load job based on file type
                if file_extension == 'csv':
                    job_config = bigquery.LoadJobConfig(
                        source_format='CSV',
                        skip_leading_rows=1,
                        autodetect=True,
                        write_disposition='WRITE_APPEND',
                        create_disposition='CREATE_NEVER',
                    )
                else:  
                    source_format = 'JSON'
                    job_config = bigquery.LoadJobConfig(
                        source_format=source_format,
                        autodetect=True,
                        write_disposition='WRITE_APPEND',
                        create_disposition='CREATE_NEVER'
                    )
                
                uri = f"gs://{LANDING_BUCKET}/{file_path}"
                table_id = f"{client.project}.{BQ_RAW_DATASET}.{table_name}"
                
                # Load data to BigQuery
                load_job = client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )
                
                # Wait for the job to complete
                load_job_result = load_job.result()
                
                # Get the number of rows ingested
                ingested_row_count = load_job_result.output_rows
                
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
                
                # Log to audit table
                current_time = datetime.now()
                audit_row = {
                    "file_name": file_name,
                    "status": "SUCCESS",
                    "error_reason": None,
                    "ingested_row_count": ingested_row_count,
                    "ingestion_time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "ingestion_id": dag_run_id
                }
                
                # Insert into audit table
                audit_rows = [audit_row]
                audit_errors = client.insert_rows_json(AUDIT_TABLE, audit_rows)
                if audit_errors:
                    print(f"Errors inserting into audit table: {audit_errors}")
                
                # Generate timestamp for the filename
                timestamp = current_time.strftime("%Y%m%d%H%M%S")
                
                # Split the file path into directory and filename
                file_dir = os.path.dirname(file_path)
                file_base, file_ext = os.path.splitext(file_name)
                
                # Create the new filename with timestamp
                timestamped_filename = f"{file_base}_{timestamp}{file_ext}"
                
                # Create the full destination path
                if file_dir:
                    destination_path = f"{file_dir}/{timestamped_filename}"
                else:
                    destination_path = timestamped_filename
                
                # Move file to archive bucket with timestamp in the filename
                gcs_hook = GCSHook()
                gcs_hook.copy(
                    source_bucket=LANDING_BUCKET,
                    source_object=file_path,
                    destination_bucket=ARCHIVE_BUCKET,
                    destination_object=destination_path
                )
                
                # Delete file from landing bucket
                gcs_hook.delete(
                    bucket_name=LANDING_BUCKET,
                    object_name=file_path
                )
                
                results.append({
                    "status": "success", 
                    "file": file_path, 
                    "archived_as": destination_path,
                    "message": f"File {file_path} processed successfully and archived as {destination_path}",
                    "dag_run_id": dag_run_id,
                    "ingested_row_count": ingested_row_count
                })
                
            except Exception as e:
                error_message = str(e)
                # On error, move file to error bucket with timestamp
                try:
                    # Log to audit table for error case
                    client = bigquery.Client()
                    current_time = datetime.now()
                    audit_row = {
                        "file_name": os.path.basename(file_path),
                        "status": "ERROR",
                        "error_reason": error_message[:1000],  # Truncate if too long
                        "ingested_row_count": 0,
                        "ingestion_time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "ingestion_id": dag_run_id
                    }
                    
                    # Insert into audit table
                    audit_rows = [audit_row]
                    audit_errors = client.insert_rows_json(AUDIT_TABLE, audit_rows)
                    if audit_errors:
                        print(f"Errors inserting into audit table: {audit_errors}")
                    
                    # Generate timestamp for the filename
                    timestamp = current_time.strftime("%Y%m%d%H%M%S")
                    
                    # Split the file path into directory and filename
                    file_dir = os.path.dirname(file_path)
                    file_name = os.path.basename(file_path)
                    file_base, file_ext = os.path.splitext(file_name)
                    
                    # Create the new filename with timestamp
                    timestamped_filename = f"{file_base}_{timestamp}{file_ext}"
                    
                    # Create the full destination path
                    if file_dir:
                        destination_path = f"{file_dir}/{timestamped_filename}"
                    else:
                        destination_path = timestamped_filename
                    
                    gcs_hook = GCSHook()
                    gcs_hook.copy(
                        source_bucket=LANDING_BUCKET,
                        source_object=file_path,
                        destination_bucket=ERROR_BUCKET,
                        destination_object=destination_path
                    )
                    
                    # Delete file from landing bucket
                    gcs_hook.delete(
                        bucket_name=LANDING_BUCKET,
                        object_name=file_path
                    )
                    
                    results.append({
                        "status": "error", 
                        "file": file_path,
                        "error_file": destination_path,
                        "message": f"Error processing file: {error_message}. Moved to error bucket as {destination_path}",
                        "dag_run_id": dag_run_id,
                        "ingested_row_count": 0
                    })
                    
                except Exception as move_error:
                    # Try to log to audit table even if move failed
                    try:
                        client = bigquery.Client()
                        current_time = datetime.now()
                        audit_row = {
                            "file_name": os.path.basename(file_path),
                            "status": "ERROR",
                            "error_reason": f"Processing error: {error_message[:500]}... Move error: {str(move_error)[:500]}",
                            "ingested_row_count": 0,
                            "ingestion_time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
                            "ingestion_id": dag_run_id
                        }
                        
                        # Insert into audit table
                        audit_rows = [audit_row]
                        audit_errors = client.insert_rows_json(AUDIT_TABLE, audit_rows)
                        if audit_errors:
                            print(f"Errors inserting into audit table: {audit_errors}")
                    except Exception as audit_error:
                        print(f"Failed to log to audit table: {str(audit_error)}")
                    
                    results.append({
                        "status": "error", 
                        "file": file_path, 
                        "message": f"Error processing file and moving to error bucket: {error_message}, Move error: {str(move_error)}",
                        "dag_run_id": dag_run_id,
                        "ingested_row_count": 0
                    })
        
        return results
    
    @task
    def summarize_results(results: List[Dict[str, str]]) -> Dict[str, int]:
        """Summarize processing results."""
        success_count = sum(1 for r in results if r["status"] == "success")
        error_count = sum(1 for r in results if r["status"] == "error")
        total_rows_ingested = sum(r.get("ingested_row_count", 0) for r in results)
        
        # Get the DAG run ID from the first result (they all have the same run ID)
        dag_run_id = results[0]["dag_run_id"] if results else "unknown"
        
        return {
            "total_files": len(results),
            "success_count": success_count,
            "error_count": error_count,
            "total_rows_ingested": total_rows_ingested,
            "dag_run_id": dag_run_id
        }
    
    # Define the workflow
    file_list = list_files()
    results = process_all_files(file_list)
    summary = summarize_results(results)

# Create the DAG
file_processing_dag_instance = file_processing_dag()