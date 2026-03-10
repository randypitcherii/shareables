resource "google_storage_bucket" "experiment" {
  name                        = var.bucket_name
  location                    = var.gcp_region
  force_destroy               = true
  uniform_bucket_level_access = true

  labels = {
    purpose = "experiment"
    owner   = "randy-pitcher"
  }
}

# Upload sample CSV data
resource "google_storage_bucket_object" "sample_csv" {
  name    = "sample-data/data.csv"
  bucket  = google_storage_bucket.experiment.name
  content = <<-CSV
    id,name,value,created_at
    1,alpha,100,2025-01-15
    2,bravo,200,2025-02-20
    3,charlie,300,2025-03-10
    4,delta,400,2025-04-05
    5,echo,500,2025-05-25
    6,foxtrot,600,2025-06-14
    7,golf,700,2025-07-08
    8,hotel,800,2025-08-19
    9,india,900,2025-09-30
    10,juliet,1000,2025-10-12
  CSV
}