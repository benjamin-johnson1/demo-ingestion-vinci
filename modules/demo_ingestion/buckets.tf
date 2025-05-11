resource "google_storage_bucket" "ingestion_buckets" {
  for_each  = local.buckets
  name  = "${local.project_id}-${each.value.name}"
  storage_class = each.value.storage_class
  location  = local.location
  force_destroy = false
}