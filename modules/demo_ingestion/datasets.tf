resource "google_bigquery_dataset" "common_datasets" {
  for_each      = local.common_datasets
  location      = local.location
  dataset_id    = "d_${local.project_name}_${each.key}_${lower(local.location)}_${lower(local.environment)}"
  friendly_name = "d_${local.project_name}_${each.key}_${lower(local.location)}_${lower(local.environment)}"
  description   = each.value
}