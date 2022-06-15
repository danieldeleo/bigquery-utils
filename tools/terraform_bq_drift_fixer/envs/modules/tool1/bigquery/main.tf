module "bq_module" {
  source  = "terraform-google-modules/bigquery/google"
  project_id = var.project_id
  dataset_id = var.dataset_id
  tables = var.tables
  views = var.views
  deletion_protection = var.deletion_protection
}