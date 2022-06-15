terraform {
  source = "${get_parent_terragrunt_dir()}/../modules/tool1/bigquery"
}

include "root" {
  path   = find_in_parent_folders()
  expose = true
}

locals {
  dataset_id = "tool1_dataset_qa"
}

# Indicate the input values to use for the variables of the module.
inputs = {
  authorizer_dataset_id = local.dataset_id
  project_id = include.root.inputs.project_id
  dataset_id    = "${local.dataset_id}"
  tables = [
    {
      table_id            = "sample_table"
      dataset_id          = "${local.dataset_id}"
      schema              = file("./json_schemas/sample_table.json")
      clustering          = []
      expiration_time     = null
      deletion_protection = false
      range_partitioning  = null
      time_partitioning   = null
      labels              = {}
    }
  ]
}