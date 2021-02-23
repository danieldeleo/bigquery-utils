# BigQuery Audit Metadata

Learn how to leverage [BigQueryAuditMetadata](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata) for advanced BigQuery usage analysis. This directory is an upgrade to our previous [bigquery_audit_log_v1.sql](/views/audit/bigquery_audit_log_v1.sql) as it reads the newer and more detailed BigQueryAuditMetadata events; including information such as which tables were read/written by a given query job or which tables expired due to having an expiration time configured. The main file is: 
* __[bigquery_audit_log_v2.sql](/views/audit/bigquery_audit_log_v2.sql)__, a SELECT statement to help you extract and format metadata
events

## Getting Started

Console commands, including `gcloud` and `bq`,  should be run in the Cloud Shell of the project you are working in. Several steps are performed through the Console UI.

1.  Set your environment variables, replace <your-project> and <your-dataset> with the appropriate information
    ```
    export PROJECT=<your-project>
    export DATASET=<your-dataset>
    ```
2.  Create a BigQuery dataset to store the audit logs, the cloudaudit table will be populated once you run a BigQuery job post log sink creation  
    ```
    bq --project_id $PROJECT mk $DATASET
    ```  
3.  Define a log sink using any of the following methods:
    *   [gcloud command](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud) (gcloud **alpha** is needed in order to use the parameter `--use-partitioned-tables`)
        ```
        gcloud alpha logging sinks create bq-auditv2-sinky bigquery.googleapis.com/projects/$PROJECT/datasets/$DATASET --log-filter='protoPayload.metadata."@type"="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata"' --use-partitioned-tables
        ```  
    *   [Cloud Console Logs Viewer](https://cloud.google.com/logging/docs/export/configure_export_v2#dest-create) (Make sure to select [partitioning](https://cloud.google.com/logging/docs/export/bigquery#partition-tables)for your BigQuery destination
    > Note: You can create a log sink at the folder, billing account, or organization level using an [aggregated sink](https://cloud.google.com/logging/docs/export/aggregated_sinks#creating_an_aggregated_sink).
4.  Assign the log sink's service account the BigQuery Data Editor role
    * In the console, navigate to Logging >> Logs Router 
    * Click the 3-dot menu for the sink you created above and select *View sink details*
    * Copy Writer identity, e.g. `serviceAccount:pXXXXXX@gcp-sa-logging.iam.gserviceaccount.com`
    * IAM & Admin >> IAM >> ADD 
    * For the member, input `pXXXXXX@gcp-sa-logging.iam.gserviceaccount.com` (part of the Writer identity you copied in a previous step)
    * Assign the Bigquery Data Editor role and press Save
5.  Update [bigquery_audit_log_v2.sql](/views/audit/bigquery_audit_log_v2.sql)
    ```
    git clone https://github.com/GoogleCloudPlatform/bigquery-utils.git
    cd bigquery-utils/views/audit
    sed -i 's/project_id.dataset_id/<your-project>.<your-dataset>/' bigquery_audit_log_v2.sql
    ```
    > Note: You must manually set project_id and dataset_id if you are using the Cloud Shell. Alternatively, you could download the file to your desktop and use a text editor to find/replace.
6.  Copy the entire update file, paste into the Bigquery query editor, and run
7.  Congratulations! From here, you can do further analysis in BigQuery by saving and querying the view, or you can connect it to a BI tool such as DataStudio as a data source and build dashboards.
    
## Usage Examples

#### Retrieve 100 Latest SELECT SQL Queries Executed 
  ```  
  SELECT * EXCEPT(
    modelDeletion,
    modelCreation,
    modelMetadataChange,
    routineDeletion,
    routineCreation,
    routineChange)
  FROM `project_id.dataset_id.audit_logs`
  WHERE 
    hasJobChangeEvent
    AND jobChange.jobConfig.queryConfig.statementType = 'SELECT'
  ORDER BY jobChange.jobStats.startTime DESC
  LIMIT 100
  ``` 
  
#### Retrieve 100 Latest DML SQL Queries Executed 
  ```  
  SELECT * EXCEPT(
    modelDeletion,
    modelCreation,
    modelMetadataChange,
    routineDeletion,
    routineCreation,
    routineChange)
  FROM `project_id.dataset_id.audit_logs`
  WHERE 
    hasJobChangeEvent
    AND jobChange.jobConfig.queryConfig.statementType IN (
          'INSERT', 'DELETE', 'UPDATE', 'MERGE')
  ORDER BY jobChange.jobStats.startTime DESC
  LIMIT 100
  ``` 
  
#### Retrieve 100 Latest BigQuery Load Jobs
  ```
  SELECT * EXCEPT(
    modelDeletion,
    modelCreation,
    modelMetadataChange,
    routineDeletion,
    routineCreation,
    routineChange)
  FROM `project_id.dataset_id.audit_logs`
  WHERE 
    hasJobChangeEvent AND 
    jobChange.jobConfig.loadConfig.destinationTable IS NOT NULL
  ORDER BY jobChange.jobStats.startTime DESC
  LIMIT 100
  ```
  
#### Retrieve 100 Latest BigQuery Table Deletion Events
  ```
  SELECT * EXCEPT(
    modelDeletion,
    modelCreation,
    modelMetadataChange,
    routineDeletion,
    routineCreation,
    routineChange)
  FROM `project_id.dataset_id.audit_logs`
  WHERE 
    hasTableDeletionEvent
  ORDER BY jobChange.jobStats.startTime DESC
  LIMIT 100
  ```
