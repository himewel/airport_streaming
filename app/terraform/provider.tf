provider "google" {
  credentials = file(var.CREDENTIALS_FILEPATH)
  project     = var.PROJECT_ID
  zone        = var.PROJECT_ZONE
}
