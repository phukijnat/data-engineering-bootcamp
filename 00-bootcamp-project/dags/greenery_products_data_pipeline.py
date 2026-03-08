import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from airflow.providers.postgres.hooks.postgres import PostgresHook


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "dataengineer-bootcamp"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "products"
keyfile_gcs = f"{DAGS_FOLDER}/deb-uploading-files-to-gcs.json"
keyfile_bigquery = f"{DAGS_FOLDER}/deb-loading-data-to-bq.json"


def _extract_data():
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="greenery"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    sql = f"""
        SELECT product_id, name, price, inventory
        FROM {DATA}
    """
    cursor.execute(sql)
    rows = cursor.fetchall()

    with open(f"{DAGS_FOLDER}/{DATA}.csv", "w") as f:
        writer = csv.writer(f)
        header = [
            "product_id",
            "name",
            "price",
            "inventory",
        ]
        writer.writerow(header)
        writer.writerows(rows)


def _load_data_to_gcs():
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb-bootcamp-37"
    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


def _load_data_from_gcs_to_bigquery():
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )

    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[
            bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("name", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("price", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("inventory", bigquery.SqlTypeNames.INTEGER),
        ],
    )

    bucket_name = "deb-bootcamp-37"
    destination_blob_name = f"cleaned/{BUSINESS_DOMAIN}/{DATA}/*.parquet"
    table_id = f"{GCP_PROJECT_ID}.deb_bootcamp.{DATA}"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id="greenery_products_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs
    )
    
    # Submit a Spark app to transform data
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application=f"/opt/spark/pyspark/transform_{DATA}.py",
        conn_id="my_spark",
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery