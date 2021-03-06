# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataProcPySparkOperator, DataprocClusterDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators import BashOperator, PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

##################################################################
# This file defines the DAG for the logic pictured below.        #
##################################################################
#                                                                #
#                       create_cluster                           #
#                             |                                  #
#                             V                                  #
#                       submit_pyspark.......                    #
#                             |             .                    #
#                            / \            V                    #
#                           /   \      move_failed_files         #
#                          /     \          ^                    #
#                          |     |          .                    #
#                          V     V          .                    #
#             delete_cluster     bq_load.....                    #
#                                   |                            #
#                                   V                            #
#                         delete_transformed_files               #
#                                                                #
# (Note: Dotted lines indicate conditional trigger rule on       #
# failure of the up stream tasks. In this case the files in the  #
# raw-{timestamp}/ GCS path will be moved to a failed-{timestamp}#
# path.)                                                         #
##################################################################

# Airflow parameters, see https://airflow.incubator.apache.org/code.html
DEFAULT_DAG_ARGS = {
    'owner': 'airflow',  # The owner of the task.
    # Task instance should not rely on the previous task's schedule to succeed.
    'depends_on_past': False,
    # We use this in combination with schedule_interval=None to only trigger the DAG with a
    # POST to the REST API.
    # Alternatively, we could set this to yesterday and the dag will be triggered upon upload to the
    # dag folder.
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': timedelta(minutes=1),  # Time between retries.
    'project_id': Variable.get('gcp_project'),  # Cloud Composer project ID.
    # We only want the DAG to run when we POST to the api.
    # Alternatively, this could be set to '@daily' to run the job once a day.
    # more options at https://airflow.apache.org/scheduler.html#dag-runs
}

# Create Directed Acyclic Graph for Airflow
with DAG('sparkify',
         default_args=DEFAULT_DAG_ARGS,
         schedule_interval=None) as dag:  # Here we are using dag as context.
    # Create the Cloud Dataproc cluster.
    # Note: this operator will be flagged a success if the cluster by this name already exists.
    create_cluster = DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # ds_nodash is an airflow macro for "[Execution] Date string no dashes"
        # in YYYYMMDD format. See docs https://airflow.apache.org/code.html?highlight=macros#macros
        cluster_name='ephemeral-spark-cluster-{{ ds_nodash }}',
        image_version='1.5-debian10',
        num_workers=0,
        master_machine_type='n1-standard-1',
        num_masters=1,
        storage_bucket='dataproc-staging',
        zone='us-central1-b')

    # Submit the PySpark job.
    submit_pyspark = DataProcPySparkOperator(
        task_id='run_dataproc_pyspark',
        main='gs://egen-training-286300' +
        '/spark-jobs/sparkify_etl_2.py',
        # Obviously needs to match the name of cluster created in the prior Operator.
        cluster_name='ephemeral-spark-cluster-{{ ds_nodash }}',
        # Let's template our arguments for the pyspark job from the POST payload.
        arguments=[
            "--bucket={{ dag_run.conf['bucket'] }}",
            "--raw_file_name={{dag_run.conf['raw_file_name']}}"
        ])

    # Load the transformed files to a BigQuery table.
    bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id='GCS_to_BigQuery',
        bucket="{{dag_run.conf['bucket']}}",
        # Wildcard for objects created by spark job to be written to BigQuery
        # Reads the relative path to the objects transformed by the spark job from the POST message.
        source_objects=["transformed/users_table.parquet/part-*"],
        destination_project_dataset_table='test.users_table',
        autodetect=True,
        source_format='PARQUET',  # Note that our spark job does json -> csv conversion.
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        write_disposition='WRITE_TRUNCATE',  # If the table exists, overwrite it.
        max_bad_records=0)

    bash_load_times_table = BashOperator(
        task_id='load-times-to-big-query',
        bash_command="bq load --source_format PARQUET --hive_partitioning_mode=AUTO "
                     "--hive_partitioning_source_uri_prefix={{dag_run.conf['bucket']}}/transformed/time_table.parquet" +
                     "/ test.tt2 {{dag_run.conf['bucket']}}/transformed/time_table.parquet/*")

    # Delete the Cloud Dataproc cluster.
    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        # Obviously needs to match the name of cluster created in the prior two Operators.
        cluster_name='ephemeral-spark-cluster-{{ ds_nodash }}',
        # This will tear down the cluster even if there are failures in upstream tasks.
        trigger_rule=TriggerRule.ALL_DONE)

    # Delete  gcs files in the timestamped transformed folder.
    delete_transformed_files = BashOperator(
        task_id='delete_transformed_files',
        bash_command="gsutil -m rm -r gs://{{ dag_run.conf['bucket'] }}/transformed/")

    # If the spark job or BQ Load fails we rename the timestamped raw path to
    # a timestamped failed path.
    move_failed_files = BashOperator(
        task_id='move_failed_files',
        bash_command="gsutil mv gs://{{dag_run.conf['bucket']}}" +
        "/raw/ " +
        "{{dag_run.conf['bucket'] }}" +
        "/failed/",
        trigger_rule=TriggerRule.ONE_FAILED)
    # Set the dag property of the first Operators, this will be inherited by downstream Operators.

    create_cluster.dag = dag

    create_cluster.set_downstream(submit_pyspark)

    submit_pyspark.set_downstream([delete_cluster, bq_load, bash_load_times_table])

    bq_load.set_downstream(delete_transformed_files)

    move_failed_files.set_upstream([bq_load, submit_pyspark, bash_load_times_table])
