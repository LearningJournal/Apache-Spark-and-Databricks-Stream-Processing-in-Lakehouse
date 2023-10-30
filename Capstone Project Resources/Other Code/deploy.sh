JOB_ID=$(databricks jobs create --json '{"name": "ingest-process-pipeline",
         "email_notifications": {
             "no_alert_for_skipped_runs": "false"
         },
         "webhook_notifications": {},
         "timeout_seconds": 0,
         "max_concurrent_runs": 1,
         "tasks": [
             {
                 "task_key": "ingest-data",
                 "run_if": "ALL_SUCCESS",
                 "notebook_task": {
                     "notebook_path": "/exercise/data_ingestion",
                     "source": "WORKSPACE"
                 },
                 "job_cluster_key": "ingest-process-pipeline-cluster",
                 "timeout_seconds": 0,
                 "email_notifications": {},
                 "notification_settings": {
                     "no_alert_for_skipped_runs": "false",
                     "no_alert_for_canceled_runs": "false",
                     "alert_on_last_attempt": "false"
                 }
             },
             {
                 "task_key": "process-data-2014",
                 "depends_on": [
                     {
                         "task_key": "ingest-data"
                     }
                 ],
                 "run_if": "ALL_SUCCESS",
                 "notebook_task": {
                     "notebook_path": "/exercise/data-analysis",
                     "base_parameters": {
                         "year": "2014"
                     },
                     "source": "WORKSPACE"
                 },
                 "job_cluster_key": "ingest-process-pipeline-cluster",
                 "timeout_seconds": 0,
                 "email_notifications": {},
                 "notification_settings": {
                     "no_alert_for_skipped_runs": "false",
                     "no_alert_for_canceled_runs": "false",
                     "alert_on_last_attempt": "false"
                 }
             },
             {
                 "task_key": "process-data-2015",
                 "depends_on": [
                     {
                         "task_key": "ingest-data"
                     }
                 ],
                 "run_if": "ALL_SUCCESS",
                 "notebook_task": {
                     "notebook_path": "/exercise/data-analysis",
                     "base_parameters": {
                         "year": "2015"
                     },
                     "source": "WORKSPACE"
                 },
                 "job_cluster_key": "ingest-process-pipeline-cluster",
                 "timeout_seconds": 0,
                 "email_notifications": {}
             }
         ],
         "job_clusters": [
             {
                 "job_cluster_key": "ingest-process-pipeline-cluster",
                 "new_cluster": {
                     "cluster_name": "",
                     "spark_version": "14.0.x-scala2.12",
                     "spark_conf": {
                         "spark.databricks.delta.preview.enabled": "true",
                         "spark.master": "local[*, 4]",
                         "spark.databricks.cluster.profile": "singleNode"
                     },
                     "azure_attributes": {
                         "first_on_demand": 1,
                         "availability": "ON_DEMAND_AZURE",
                         "spot_bid_max_price": -1
                     },
                     "node_type_id": "Standard_DS3_v2",
                     "custom_tags": {
                         "ResourceClass": "SingleNode"
                     },
                     "enable_elastic_disk": "true",
                     "data_security_mode": "SINGLE_USER",
                     "runtime_engine": "STANDARD",
                     "num_workers": 0
                 }
             }
         ],
         "format": "MULTI_TASK"
         }' | jq '.job_id')

         echo "Successfully Deployed Job ID: $JOB_ID"