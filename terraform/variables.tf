variable "location" {
  description = "GCP region for resources"
  default     = "asia-south1"
}

variable "project_id" {
  description = "GCP Project ID"
  default     = "eastern-amp-449614-e1"
}

variable "bq_dataset_name" {
  description = "Bigquery Dataset name"
  default     = "lending_db"
}

variable "gcs_bucket_name" {
  description = "GCS storage bucket name"
  default     = "lending_ara"
}
