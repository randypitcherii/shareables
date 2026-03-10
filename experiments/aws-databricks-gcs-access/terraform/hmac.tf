# HMAC key for S3-compatible access to GCS
resource "google_storage_hmac_key" "gcs_s3_compat" {
  service_account_email = google_service_account.gcs_reader.email
}
