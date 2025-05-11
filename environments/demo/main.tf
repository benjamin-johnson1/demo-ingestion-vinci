terraform {
  required_version = ">= 1.3.4, < 2.0.0"
  backend "gcs" {
    bucket = "bj-demo-ingestion-vinci-gcs-tfstate"
    prefix = "project_name/"
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.5.0, <5.0.0"
    }
  }
}

provider "google" {
  project = local.project_id
}

locals {
  project_id = "bj-demo-ingestion-vinci"
  env = "demo"
  location = "eu"
  region = "europe-west1"
  usecase = "vinci"
  repo_name = "demo-ingestion-vinci"
  orga_name = "benjamin-johnson1" 
}


module "init" {
  source = "../../modules/init"
  project_id = local.project_id
  usecase = local.usecase
  orga_name = local.orga_name
  repo_name = local.repo_name
  env = local.env
}


module "demo_ingestion" {
  source  = "../../modules/demo_ingestion"
  project_id  = local.project_id
  usecase = local.usecase
  env = local.env
  location  = local.location
  region = local.region
}

