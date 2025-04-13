terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = "../gcp-creds.json"
  project     = var.project_id
  region      = var.location
}

resource "google_storage_bucket" "gcp-storage" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 3
    }
    
    action {
      type = "Delete"
    }
  }
}

# Create "code" folder in the bucket
resource "google_storage_bucket_object" "code_folder" {
  name    = "code/"
  content = " "  # Empty content for folder creation
  bucket  = google_storage_bucket.gcp-storage.name
}


resource "google_bigquery_dataset" "bigquery-dataset" {
  dataset_id                  = var.bq_dataset_name
  location                    = var.location
  project                     = var.project_id
  delete_contents_on_destroy  = true
  
  default_table_expiration_ms = 3600000 * 24 * 3 # 3 days
}

# Set appropriate IAM permissions for the bucket
resource "google_storage_bucket_iam_binding" "bucket_admin" {
  bucket = google_storage_bucket.gcp-storage.name
  role = "roles/storage.admin"
  members = [
    "serviceAccount:terraform-runner@${var.project_id}.iam.gserviceaccount.com",
  ]
}

