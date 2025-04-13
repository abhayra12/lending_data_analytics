output "bucket_name" {
  description = "The name of the GCS bucket"
  value       = google_storage_bucket.gcp-storage.name
}

output "bucket_url" {
  description = "The URL of the GCS bucket"
  value       = google_storage_bucket.gcp-storage.url
}

output "bigquery_dataset_id" {
  description = "The ID of the BigQuery dataset"
  value       = google_bigquery_dataset.bigquery-dataset.dataset_id
}

output "project_id" {
  description = "The GCP project ID"
  value       = var.project_id
} 