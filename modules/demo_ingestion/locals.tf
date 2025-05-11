locals {
    #general
    usecase = var.usecase
    location = var.location
    project_id = var.project_id
    env= var.env
    region = var.region
    
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

    tables = {
        t_raw_airport_info = {
            id = "t_raw_airport_info"
            description = "airport metadata ingested from json files"
            schema = file("${path.module}/schemas/t_raw_airport_info.json")
            time_partitioning = {}
            clustering = []
            dataset = "d_vinci_raw_eu_demo"
        }

        t_raw_airport_traffic = {
            id = "t_raw_airport_traffic"
            description = "airport trafic data ingested from csv files"
            schema = file("${path.module}/schemas/t_raw_airport_traffic.json")
            time_partitioning = {
                "type"    = "DAY"
                "field"   = "date"
                "require" = false
            }
            clustering = ["airport_code", "flight_type"]
            dataset = "d_vinci_raw_eu_demo"
        }
    }

}