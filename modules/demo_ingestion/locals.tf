locals {
    #general
    usecase = var.usecase
    location = var.location
    project_id = var.project_id
    env= var.env
    
    buckets = {
        landing = {
          name          = "landing"
          storage_class = "STANDARD"
        }
        archive = {
          name          = "archive"
          storage_class = "STANDARD"
        }
        error = {
          name          = "error"
          storage_class = "STANDARD"
        }
  }

    common_datasets = {
        "audit"      = "Contains ${local.usecase} audit tables (public)"
        "raw"        = "Contains ${local.usecase} raw tables (private)"
  }

}