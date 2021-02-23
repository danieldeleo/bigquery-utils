#!/usr/bin/env bash

viewSQLPath=$1
projectId=$2
dataset=$3
viewName=$4
viewSQL=$(sed "s/project_id.dataset_id/$projectId.$dataset/" < "$viewSQLPath")

bq query --nouse_legacy_sql "${viewSQL}"