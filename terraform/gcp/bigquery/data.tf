variable "GCS_BUCKET" {
  type = string
}

variable "BQ_DATASET" {
  type = string
}

variable "BQ_VIEWS" {
  type = string
}

variable "TABLE_NAME" {
  type    = list(string)
  default = [
    "dim_datas",
    "dim_digito_identificador",
    "dim_aeronaves",
    "dim_companhias",
    "dim_aerodromos",
    "fact_voos"
  ]
}
