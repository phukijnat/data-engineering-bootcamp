from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import great_expectations as gx
import pandas as pd


def _get_data():
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="greenery"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        select product_id, name, price, inventory from products
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    for each in rows:
        print(each)


def _dump_data(table: str):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="greenery"
    )
    pg_hook.bulk_dump(table, f"/opt/airflow/dags/{table}_export")


# def _validate_data():
#     print(f"GX Version: {gx.__version__}")
#     columns = ["product_id", "name", "price", "inventory"]
#     df = pd.read_csv("/opt/airflow/dags/products_export", names=columns, sep="\t")

#     context = gx.get_context()
#     data_source = context.data_sources.add_pandas("pandas")
#     data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

#     batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
#     batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

#     expectation = gx.expectations.ExpectColumnValuesToBeBetween(
#         column="price", min_value=0, max_value=1
#     )
#     validation_result = batch.validate(expectation)

#     assert validation_result["success"] is True

def _validate_data():
    print(f"GX Version: {gx.__version__}") # เช็คให้ชัวร์อีกรอบ
    
    columns = ["product_id", "name", "price", "inventory"]
    df = pd.read_csv("/opt/airflow/dags/products_export", names=columns, sep="\t")
    print("--- Check Data Types ---")
    print(df.dtypes) 
    print(f"Max Price in Data: {df['price'].max()}")
    context = gx.get_context()
    
    # 1. จุดตาย: ต้องใช้ .sources (ไม่มี underscore) ในเวอร์ชัน 0.18.x
    data_source = context.sources.add_pandas("my_pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name="pd_dataframe_asset")

    # 2. สร้าง Batch Request จาก DataFrame
    batch_request = data_asset.build_batch_request(dataframe=df)

    # 3. ใน 0.18.x ต้องใช้ Validator เป็นตัวกลางในการตรวจ (ยังใช้ batch.validate ไม่ได้)
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="my_temp_suite"
    )

    # 4. เรียก Expectation ผ่าน Validator
    validation_result = validator.expect_column_values_to_be_between(
        column="price", min_value=0, max_value=1
    )

    print(f"Success: {validation_result['success']}")
    assert validation_result["success"] is True

with DAG(
    dag_id="play_with_airflow_connections_and_hooks",
    schedule=None,
    start_date=timezone.datetime(2024, 3, 10),
	  catchup=False,
    tags=["DEB", "Skooldio"],
):

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
    )

    dump_product_data = PythonOperator(
        task_id="dump_product_data",
        python_callable=_dump_data,
        op_kwargs={"table": "products"},
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    dump_product_data >> validate_data