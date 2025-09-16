provider "google" {
  project = var.project_id
  region  = var.region
}

# Habilita APIs
resource "google_project_service" "services" {
  for_each = toset([
    "storage.googleapis.com",
    "iam.googleapis.com"
  ])
  service = each.key
}

# Bucket do lake
resource "google_storage_bucket" "bucket" {
  name                        = var.bucket_name
  location                    = var.bucket_location
  storage_class               = var.storage_class
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 3650 
    }
  }

  labels = {
    projeto     = "financial-insights"
    ambiente    = "dev"
    responsavel = "gabriel"
  }
}

resource "google_storage_bucket_object" "medallion_placeholders" {
  for_each     = toset(local.medallion_prefixes)
  bucket       = google_storage_bucket.bucket.name
  name    = "${each.value}/_README.txt"
  content = "Prefixo ${each.value} do data lake (medallion)."                           
  content_type = "application/octet-stream"
}
