output "bucket_name" {
  value = google_storage_bucket.experiment.name
}

output "bucket_url" {
  value = "gs://${google_storage_bucket.experiment.name}"
}

output "service_account_email" {
  value = google_service_account.gcs_reader.email
}

output "sa_key_json_base64" {
  value     = google_service_account_key.gcs_reader_key.private_key
  sensitive = true
}

output "hmac_access_id" {
  value     = google_storage_hmac_key.gcs_s3_compat.access_id
  sensitive = true
}

output "hmac_secret" {
  value     = google_storage_hmac_key.gcs_s3_compat.secret
  sensitive = true
}
