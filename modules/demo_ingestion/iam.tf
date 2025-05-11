resource "google_service_account" "composer_service_account" {
  account_id   = "composer-sa"
  display_name = "Cloud Composer Service Account"
}

# Grant necessary permissions to the service account
resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_service_account.email}"
}
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.composer_service_account.email}"
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.composer_service_account.email}"
}

resource "google_project_iam_member" "composer_service_agent_v2_ext" {
  project = var.project_id  # Assurez-vous d'utiliser la variable correcte pour votre ID de projet
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-673794302689@cloudcomposer-accounts.iam.gserviceaccount.com"
}