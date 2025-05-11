resource "google_bigquery_table" "table" {
  for_each = local.tables
  deletion_protection = false
  dataset_id = each.value.datatset
  table_id = each.value.id
  schema = each.value.schema
  description = each.value.description

  dynamic "time_partitioning" {
    for_each = length(each.value.time_partitioning) == 0 ? [] : ["do"]
    content {
      type = each.value.time_partitioning.type
      field = each.value.time_partitioning.field
      require_partition_filter = each.value.time_partitioning.require
    }
  }
  clustering = try(each.value.clustering, [])

}