resource "google_composer_environment" "composer_env" {
  name   = "${local.project_id}-composer"
  region = local.region
  
  config {
    node_config {
      service_account = google_service_account.composer_service_account.email
    }

    software_config {
      image_version = "composer-2.12.1-airflow-2.10.5"
      
      airflow_config_overrides = {
        core-dags_are_paused_at_creation = "False"
      }
      
      env_variables = {
        LANDING_BUCKET = "${local.project_id}-landing"
        ERROR_BUCKET   = "${local.project_id}-error"
        ARCHIVE_BUCKET = "${local.project_id}-archive"
        BQ_DATASET     = "d_${local.usecase}_raw_${lower(local.location)}_${lower(local.env)}"
      }
    }
  }

  depends_on = [
    google_project_iam_member.composer_worker,
    google_project_iam_member.storage_admin,
    google_project_iam_member.bigquery_admin
  ]
}
resource "google_storage_bucket_object" "dag_file" {
  name   = "dags/file_to_bq_dag.py"
  bucket = google_composer_environment.composer_env.config[0].dag_gcs_prefix
  source = "${path.module}/dags/file_to_bq_dag.py"
}