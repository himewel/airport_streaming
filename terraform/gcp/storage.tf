# storage bucket to store processed data
resource "google_storage_bucket" "default" {
  name = var.GCS_BUCKET
}
