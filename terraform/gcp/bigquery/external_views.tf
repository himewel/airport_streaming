# views of the external tables to be acessed by superset
resource "google_bigquery_dataset" "views" {
  dataset_id = var.BQ_VIEWS
}

resource "google_bigquery_table" "view" {
  dataset_id = google_bigquery_dataset.views.dataset_id
  table_id   = var.TABLE_NAME[count.index]
  count      = length(var.TABLE_NAME)

  view {
    query = "SELECT * FROM ${google_bigquery_dataset.dw.dataset_id}.${google_bigquery_table.table[count.index].table_id}"
    use_legacy_sql = false
  }
}
