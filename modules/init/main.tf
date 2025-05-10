variable "project_id" { type = string }
variable "usecase" { type = string }
variable "repo_name" { type = string }
variable "orga_name" { type = string }
variable "env" { type = string }

terraform {
  required_version = ">= 1.3.4, < 2.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.5.0, <5.0.0"
    }
  }
}


# Service account to get access to data from other GCP project
resource "google_service_account" "deploy" {
  project      = var.project_id
  account_id   = "${var.usecase}-sa-cloudbuild-${var.env}"
  description  = "Service account for deploying infrastructure on the project: ${var.usecase} (to be used instead of the default Cloud Build account)"
  display_name = "deploy service account"
}


resource "google_project_iam_member" "deploy_bindings" {
  project = var.project_id
  for_each = toset(["roles/bigquery.admin",
    "roles/cloudscheduler.admin",
    "roles/pubsub.admin",
    "roles/logging.logWriter",
    "roles/storage.admin",
    "roles/run.admin",
    "roles/workflows.admin",
    "roles/cloudbuild.serviceAgent",
    "roles/resourcemanager.projectIamAdmin",
    "roles/iam.serviceAccountAdmin",
    "roles/cloudbuild.builds.builder",
    "roles/monitoring.notificationChannelEditor",
  "roles/monitoring.admin"])
  role   = each.key
  member = "serviceAccount:${google_service_account.deploy.email}"
}

output "deploy" {
  value = google_service_account.deploy.email
}

resource "google_cloudbuild_trigger" "tf-plan" {
  github {
    #TO CHANGE
    ##################################################
    owner = var.orga_name
    name  = var.repo_name
    ##################################################
    pull_request {
      branch = "^(main|env/${var.env})$"
    }
  }

  substitutions = {
    _APPLY_CHANGES = "false"
    _ENV           = var.env
  }
  name = "${var.usecase}-plan"
  #description     = "Triggered for each Pull Request targeting the 'main' or 'env/${var.env}' branch"
  filename        = "cloudbuild-init.yaml"
  service_account = google_service_account.deploy.id
}

resource "google_cloudbuild_trigger" "tf-apply" {
  github {
    #TO CHANGE
    ##################################################
    owner = var.orga_name
    name  = var.repo_name
    ##################################################
    push {
      branch = "^(main)$"
      # (var.env == "pd") ? "^(main)$" : "^(integration)$" # for git flow
    }
  }

  substitutions = {
    _APPLY_CHANGES = "true"
    _ENV           = var.env
    _INIT          = "true"
  }
  name            = "${var.usecase}-apply"
  filename        = "cloudbuild-init.yaml"
  service_account = google_service_account.deploy.id
}
