#!/bin/bash

cd dataform

if [[ "${BRANCH_NAME}" != "master" ]]; then
  export ENV=test
else
  export ENV=prod
fi

echo "{\"projectId\": \"${PROJECT_ID}\", \"location\": \"${BQ_LOCATION}\"}" > .df-credentials.json
cp "${ENV}_dataform.json" dataform.json

dataform install

# Need to specify a separate flag for each individual action/tag/var value ie. dataform run --tags example1 --tags example2 --tags example3
#  https://github.com/dataform-co/dataform/issues/1200
# The bash line below replaces all occurrences of comma
# in $DATAFORM_TAGS with the string '--tags'
# Bash parameter expansion is used to do this:
#   ${parameter//find/replace}
# https://www.gnu.org/software/bash/manual/html_node/Shell-Parameter-Expansion.html
if [[ -n $DATAFORM_TAGS ]]; then
  all_dataform_tags="--tags ${DATAFORM_TAGS//,/ --tags }"
fi

if [[ -n $DATAFORM_ACTIONS ]]; then
  all_dataform_actions="--actions ${DATAFORM_ACTIONS//,/ --tags }"
fi

printf """
Executing the following dataform command:
************************************************************
dataform run %s %s
************************************************************
""" "${all_dataform_tags}" "${all_dataform_actions}"
dataform run --run-tests $(echo "${all_dataform_tags}" "${all_dataform_actions}" | xargs)
