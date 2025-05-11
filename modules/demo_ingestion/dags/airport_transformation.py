from datetime import datetime, timedelta
from airflow.decorators import dag, task
from google.cloud import bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': "benjamin99.johnson@gmail.com",
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='data_transformation_processor',
    default_args=default_args,
    description='Transform raw data loaded by file_to_bq_processor',
    schedule_interval=None,  # Ce DAG sera déclenché par TriggerDagRunOperator
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def data_transformation_dag():
    
    @task
    def run_transformation_query():
        """Run the transformation query."""
        client = bigquery.Client()
        
        transformation_query = """
        -- Example: Transform airport_info data
        INSERT INTO `bj-demo-ingestion-vinci.d_vinci_warehouse_eu_demo.t_warehouse_airport` 
        SELECT 
            traffic.airport_code,
            info.full_name,
            info.country,
            info.city,
            info.size,
            info.max_capacity,
            traffic.date,
            traffic.flight_type,
            SUM(traffic.passenger_count) as total_passenger_count
        FROM `bj-demo-ingestion-vinci.d_vinci_raw_eu_demo.t_raw_airport_traffic` as traffic
        LEFT JOIN (SELECT DISTINCT 
                    airport_code,
                    full_name,
                    country,
                    city,
                    size,
                    max_capacity
                    FROM `bj-demo-ingestion-vinci.d_vinci_raw_eu_demo.t_raw_airport_info`) as info
        ON UPPER(traffic.airport_code) = UPPER(info.airport_code)
        GROUP BY 
            traffic.airport_code,
            info.full_name,
            info.country,
            info.city,
            info.size,
            info.max_capacity,
            traffic.date,
            traffic.flight_type;
        """
        
        # Run the query
        query_job = client.query(transformation_query)
        
        # Wait for the job to complete
        query_job.result()
        
        print("Transformation query completed successfully")
    
    # Define the workflow
    transform_task = run_transformation_query()

# Create the DAG
data_transformation_dag_instance = data_transformation_dag()