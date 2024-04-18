module "bigquery-dataset" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric//modules/bigquery-dataset?ref=v30.0.0&depth=1"
  project_id = "danny-bq"
  id         = "example_dataset"
  location   = "US"
  access = {
    owner          = { role = "OWNER", type = "user" }
  }
  access_identities = {
    owner          = "ddeleo@google.com"
  }
}