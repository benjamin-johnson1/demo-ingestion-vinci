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
  location = "EU"
  region = "europe-west1"
  usecase = "demo-ingestion-vinci"
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

/* TODO: Uncomment this module after the project initialization
module "demo_ingestion" {
  source          = "../../modules/template"
  env             = local.env
  project_id      = local.project_id
  location        = local.location
  region          = local.region
  deploy_sa       = module.init.deploy
}
*/
