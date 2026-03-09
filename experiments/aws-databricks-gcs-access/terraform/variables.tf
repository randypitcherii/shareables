variable "gcp_project" {
  description = "GCP project ID"
  type        = string
  default     = "gcp-sandbox-field-eng"
}

variable "gcp_region" {
  description = "GCP region for the bucket"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Name for the GCS test bucket"
  type        = string
  default     = "dbx-gcs-access-experiment"
}

variable "service_account_id" {
  description = "ID for the GCP service account"
  type        = string
  default     = "dbx-gcs-reader"
}
