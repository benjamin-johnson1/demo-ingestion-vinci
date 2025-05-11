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
        "audit"      = "Contains ${local.usecase} audit tables "
        "raw"        = "Contains ${local.usecase} raw tables "
        "warehouse"  = "Contains ${local.usecase} transformed warehouse tables"
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

        t_warehouse_airport = {
            id = "t_warehouse_airport"
            description = "airport trafic data denormalized"
            schema = file("${path.module}/schemas/t_warehouse_airport.json")
            time_partitioning = {
                "type"    = "DAY"
                "field"   = "date"
                "require" = false
            }
            clustering = ["airport_code", "flight_type"]
            dataset = "d_vinci_warehouse_eu_demo"
        }

        t_audit_ingestion = {
            id = "t_audit_ingestion"
            description = "audit logs ingestion"
            schema = file("${path.module}/schemas/t_audit_ingestion.json")
            time_partitioning = {}
            clustering = []
            dataset = "d_vinci_audit_eu_demo"
        }
    }

}