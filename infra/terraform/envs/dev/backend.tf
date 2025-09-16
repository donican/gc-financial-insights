terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 7.2.0"
    }
  }
  # backend local (padrão). depois migramos para GCS se quiser.
}
