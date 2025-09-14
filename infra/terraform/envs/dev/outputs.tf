output "bucket_name" {
  value       = google_storage_bucket.bucket.name
  description = "Nome do bucket criado"
}

output "bucket_url" {
  value       = "gs://${google_storage_bucket.bucket.name}"
  description = "URL gs:// do bucket"
}
