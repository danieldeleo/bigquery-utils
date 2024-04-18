terraform {
  backend "gcs" {
    bucket  = "danny-bq"
    prefix  = "terraform/state/"
  }
}