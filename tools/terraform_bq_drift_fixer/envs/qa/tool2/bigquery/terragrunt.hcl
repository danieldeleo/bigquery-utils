terraform {
  source = "${get_parent_terragrunt_dir()}/../modules/tool2/bigquery"
}

include "root" {
  path   = find_in_parent_folders()
  expose = true
}

locals {
  dataset_id = "tool2_dataset_qa"
}

inputs = {
  project_id = include.root.inputs.project_id
  # The ID of the project in which the resource belongs. If it is not provided, the provider project is used.
  dataset_id    = "${local.dataset_id}"
  tables = [
    {
      table_id            = "sample_table"
      dataset_id          = "${local.dataset_id}"
      schema              = file("./json_schemas/sample_table.json")
      clustering          = []
      expiration_time     = null
      deletion_protection = true
      range_partitioning  = null
      time_partitioning   = null
      labels              = {}
    }
  ]
}


