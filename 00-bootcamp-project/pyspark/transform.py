from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, TimestampType


BUSINESS_DOMAIN = "greenery"
BUCKET_NAME = "deb-bootcamp-37"
KEYFILE_PATH = "/opt/spark/pyspark/deb-uploading-files-to-gcs.json"

# GCS Connector Path (on Spark): /opt/spark/jars/gcs-connector-hadoop3-latest.jar
# GCS Connector Path (on Airflow): /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/gcs-connector-hadoop3-latest.jar
# spark = SparkSession.builder.appName("demo") \
#     .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
#     .config("spark.memory.offHeap.enabled", "true") \
#     .config("spark.memory.offHeap.size", "5g") \
#     .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("google.cloud.auth.service.account.enable", "true") \
#     .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
#     .getOrCreate()

spark = SparkSession.builder.appName("transform") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# Example schema for Greenery users data
# struct_schema = StructType([
#     StructField("user_id", StringType()),
#     StructField("first_name", StringType()),
#     StructField("last_name", StringType()),
#     StructField("email", StringType()),
#     StructField("phone_number", StringType()),
#     StructField("created_at", TimestampType()),
#     StructField("updated_at", TimestampType()),
#     StructField("address_id", StringType()),
# ])

struct_schema = {
    "addresses": StructType([
        StructField("address_id", StringType()),
        StructField("address", StringType()),
        StructField("zipcode", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
    ]),
    "events": StructType([
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("page_url", StringType()),
        StructField("created_at", TimestampType()),
        StructField("event_type", StringType()),
        StructField("user", StringType()),
        StructField("order", StringType()),
        StructField("product", StringType()),
    ]),
    "order_items": StructType([
        StructField("order_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
    ]),
    "orders": StructType([
        StructField("order_id", StringType()),
        StructField("created_at", TimestampType()),
        StructField("order_cost", FloatType()),
        StructField("shipping_cost", FloatType()),
        StructField("order_total", FloatType()),
        StructField("tracking_id", StringType()),
        StructField("shipping_service", StringType()),
        StructField("estimated_delivery_at", TimestampType()),
        StructField("delivered_at", TimestampType()),
        StructField("status", StringType()),
        StructField("user", StringType()),
        StructField("promo", StringType()),
        StructField("address", StringType()),
    ]),
    "products": StructType([
        StructField("product_id", StringType()),
        StructField("name", StringType()),
        StructField("price", DoubleType()),
        StructField("inventory", IntegerType()),
    ]),
    "promos": StructType([
        StructField("promo_id", StringType()),
        StructField("discount", IntegerType()),
        StructField("status", StringType()),
    ]),
    "users": StructType([
        StructField("user_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("email", StringType()),
        StructField("phone_number", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("address_id", StringType()),
    ])
}

normal_data =["addresses", "order_items", "products", "promos"]
for data in normal_data:
    GCS_FILE_PATH = f"gs://{BUCKET_NAME}/raw/{BUSINESS_DOMAIN}/{data}/{data}.csv"
    df = spark.read \
        .option("header", True) \
        .schema(struct_schema[data]) \
        .csv(GCS_FILE_PATH)
    df.show()

    df.createOrReplaceTempView(data)
    result = spark.sql(f"""
        select
            *

        from {data}
    """)

    OUTPUT_PATH = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{data}"
    result.write.mode("overwrite").parquet(OUTPUT_PATH)

partitioned_data = ["events", "orders", "users"]
for data in partitioned_data:
    dt = "2020-10-23" if data == "users" else "2021-02-10"
    GCS_FILE_PATH = f"gs://{BUCKET_NAME}/raw/{BUSINESS_DOMAIN}/{data}/{dt}/{data}.csv"
    df = spark.read \
        .option("header", True) \
        .schema(struct_schema[data]) \
        .csv(GCS_FILE_PATH)
    df.show()

    df.createOrReplaceTempView(data)
    result = spark.sql(f"""
        select
            *

        from {data}
    """)

    OUTPUT_PATH = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{data}/{dt}"
    result.write.mode("overwrite").parquet(OUTPUT_PATH)