#!/bin/bash

ENV=$1
TOOL=$2

if [[ -z "${ENV}" && -z "${TOOL}" ]]; then
  #terragrunt run-all apply -refresh-only --terragrunt-non-interactive
  terragrunt run-all apply --terragrunt-non-interactive
else
#  terragrunt run-all plan -json -refresh-only --terragrunt-non-interactive --terragrunt-working-dir="${ENV}"/"${TOOL}" > terraform_state_refresh.json
#  terragrunt run-all plan -refresh-only --terragrunt-non-interactive --terragrunt-working-dir="${ENV}"/"${TOOL}"
#  terragrunt run-all show -json --terragrunt-non-interactive --terragrunt-working-dir="${ENV}"/"${TOOL}" > state.json
#  terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${ENV}"/"${TOOL}" > terraform_plan_out.json
  #terragrunt run-all plan --terragrunt-non-interactive --terragrunt-working-dir="${ENV}"/"${TOOL}"
  terragrunt run-all apply --terragrunt-non-interactive --terragrunt-working-dir="${ENV}"/"${TOOL}"
fi

# Uncomment below to purge the terragrunt caches
 find . -type d -name ".terragrunt-cache" -prune -exec rm -rf {} \;