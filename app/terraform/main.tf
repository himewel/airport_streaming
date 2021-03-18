# resource "google_storage_bucket" "default" {
#   name = var.BUCKET_NAME
# }

resource "google_bigquery_dataset" "default" {
  dataset_id = var.DATASET_ID
}

resource "google_bigquery_table" "table" {
  dataset_id = google_bigquery_dataset.default.dataset_id
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
