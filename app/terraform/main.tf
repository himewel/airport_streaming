# resource "google_storage_bucket" "default" {
#   name = var.BUCKET_NAME
# }

resource "google_bigquery_dataset" "dw" {
  dataset_id = var.DATASET_ID
}

resource "google_bigquery_table" "table" {
  dataset_id = google_bigquery_dataset.dw.dataset_id
  table_id   = var.TABLE_NAME[count.index]
  count = length(var.TABLE_NAME)

  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris = [
      "gs://${var.BUCKET_NAME}/dw/${var.TABLE_NAME[count.index]}/*.csv"
    ]
  }
}

resource "google_bigquery_dataset" "views" {
  dataset_id = var.VIEWS_DATASET_ID
}

resource "google_bigquery_table" "view" {
  dataset_id = google_bigquery_dataset.views.dataset_id
  table_id   = var.TABLE_NAME[count.index]
  count = length(var.TABLE_NAME)

  view {
    query = "SELECT * FROM ${var.DATASET_ID}.${var.TABLE_NAME[count.index]}"
    use_legacy_sql = false
  }
}
