# external tables mirroring storage files
resource "google_bigquery_dataset" "dw" {
  dataset_id = var.BQ_DATASET
}

resource "google_bigquery_table" "table" {
  dataset_id = google_bigquery_dataset.dw.dataset_id
  table_id   = var.TABLE_NAME[count.index]
  count      = length(var.TABLE_NAME)

  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris = [
      "gs://${var.BUCKET_NAME}/dw/${var.TABLE_NAME[count.index]}/*.csv"
    ]
  }
}
