resource "google_service_account" "gcs_reader" {
  account_id   = var.service_account_id
  display_name = "Databricks GCS Reader (experiment)"
  description  = "Service account for testing GCS access from AWS Databricks"
}

# Grant the SA read access to the experiment bucket
resource "google_storage_bucket_iam_member" "reader" {
  bucket = google_storage_bucket.experiment.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.gcs_reader.email}"
}

# Also grant write access for write-back tests
resource "google_storage_bucket_iam_member" "writer" {
  bucket = google_storage_bucket.experiment.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.gcs_reader.email}"
}

# Generate a JSON key for the service account
resource "google_service_account_key" "gcs_reader_key" {
  service_account_id = google_service_account.gcs_reader.name
}
