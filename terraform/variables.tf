variable "location" {
  description = "My location"
  default = "asia-south1"
}

variable "bq_dataset_name"{
    description = "Bigquery Dataset name"
    default = "lending_db"  # Change this
}



variable "gcs_bucket_name" {
  description = "GCS storage bucket name"
  default = "lending_ara"  # Change this (use only lowercase and hyphens)
}
