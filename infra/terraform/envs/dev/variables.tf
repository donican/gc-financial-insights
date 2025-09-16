variable "project_id" {
  type        = string
  description = "ID do projeto GCP"
}

variable "region" {
  type        = string
  default     = "us-east1"
  description = "Região padrão de recursos"
}

variable "bucket_name" {
  type        = string
  description = "Nome global do bucket"
}

variable "bucket_location" {
  type        = string
  default     = "US"
  description = "Localização do bucket"
}

variable "storage_class" {
  type        = string
  default     = "STANDARD"
  description = "Classe de armazenamento (STANDARD, NEARLINE, COLDLINE, ARCHIVE)"
}
