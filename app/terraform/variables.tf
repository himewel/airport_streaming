variable "BUCKET_NAME" {
  type = string
}

variable "DATASET_ID" {
  type = string
}

variable "TABLE_NAME" {
  type = list(string)
}

variable "CREDENTIALS_FILEPATH" {
  type        = string
  description = "Filepath to the credentials json"
}

variable "PROJECT_ID" {
  type        = string
  description = "Id of the GCP project"
}

variable "PROJECT_ZONE" {
  type        = string
  description = "Default zone to create the instances"
}
