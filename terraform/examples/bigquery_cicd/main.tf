provider "google" {
  project = var.project_id
}

terraform {
  backend "gcs" {
    bucket = "dannybq"
    prefix = "terraform/state"
  }
}

data "terraform_remote_state" "foo" {
  backend = "gcs"
  config = {
    bucket = "dannybq"
    prefix = "terraform/state"
  }
}

resource "google_bigquery_dataset" "example_dataset" {
  dataset_id                  = var.dataset_id
  friendly_name               = "test"
  description                 = "This is a test description"
  location                    = "US"
  default_table_expiration_ms = 3600000
}

resource "google_bigquery_dataset_iam_binding" "reader" {
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"

  members = [
    "user:ddeleo@google.com",
  ]
}

module "tables" {
  source = "./tables"
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
}

module "views" {
  source = "./views"
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
}


resource "google_service_account" "service_account" {
  account_id = "scheduler-srvc-acct"
}

resource "google_project_iam_binding" "service_account" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${google_service_account.service_account.email}"
  ]
}

resource "google_bigquery_data_transfer_config" "query_config" {
  # Scheduled query will run as service account set below.
  # Runtime terraform identity needs permissions to act as this service account
  service_account_name = resource.google_service_account.service_account.email
  display_name         = "scheduled-dedup-query"
  location             = "us-central1"
  data_source_id       = "scheduled_query"
  # Formatting Schedule:
  # https://cloud.google.com/appengine/docs/flexible/python/scheduling-jobs-with-cron-yaml#formatting_the_schedule
  schedule = "every day 02:00"
  params = {
    # Use file function to pass query from file in git repo
    # https://www.terraform.io/docs/language/functions/file.html
    query = file("${path.module}/scheduled_queries/dedup.sql")
  }
}
