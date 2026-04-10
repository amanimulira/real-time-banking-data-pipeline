import os
import boto3
import snowflake.connector
from airflow import DAG
from cryptography.hazmat.primitives import serialization
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

PRIVATE_KEY_CONTENT = """-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQzLkhb312jvhme5SU
Ex5orwICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQIEDN+4GJgdlsEggTI
M7LgSR0ifTlr1FnWrwpLU7Z73t6b2/9OiLi5VmtbQKQ6B+UzDvHSOgNGtAAkUTVn
anJ+luVc8RFgGsRBtkdmDqGWVG7huimROC1p4IFfqn+tLG4qvwJn9snULTq3lqWU
pIE9OXGwtWXueSQ32VzCye3p3skxe3mdY0PekFIPff5YYkLX27YrM0GUiskiko8K
Ia5xym31zI994Ld+k33sBskLtx7yipjzr31BX5U2ddMV7qJFMwethsNXzIl0ie0W
hILrGsBMvUw37TzTjr+uX565jDStAdMoNVBz6wLfQVOcKVfGXpehHJDg+Ut0yaJ+
0iqi627VijU+lQI9vOsmVk/2EQX4kMZat9tM4Mb1XcPduwdFokLBQRqaRDC/fQ3i
ANWGOJiFJ7OGYGTkKjVY7IMhEUDz47jxb3nJTfC1YptaoXIJN4CzchriNFuQGAOE
JVDIeNsy6vCPNZ21PyE2ARJWn/RzwVlLl/LbWY6pTVWeYIha68JQB8HSK/dmEv/B
UqFtQWsHoRJvEH7u0ep5wNnGXWBLBIKqNQswF9ML2RcP82EnXa3vHXiRRuLgvWHy
B2Boh8SFALcuRCGwNR7qMomS4jzSXrnr2iFEBWhdnLP6fFI+qMgX6JBdQx2I7irt
KCDfetJS3ddNmOwZmgELxaFHiy2B6oW4vyutGarjDO/jQmdu6AM0Ody1x81ojZPK
kzR8anfA5Krcky7X7DDWfexd1w8AR82Tux8R9I321ASwI3XDMjJnxfHJwYhuRmUD
HyuJbDhG+YxMYx9r0YwBj4jMXs5l5f30+iSpbJt0DMbDNFAhPmZAIhd36uU2vdTh
ibQ0X0uBUwJyDQ9hy7/Zol9PwDPUDWjLT4aVPQ6ThCdPE3heWnTwC4IAtQw3QFZ4
O5QITtDQoMQ7anOplXlHJEvZN93Ej92IhShIU6C5A2DXwHAc4RLIk9FoAm6AZA7V
4w+zGsM/AYKL/8Qj7Qd/PZuVzSMUlpkkHj869oI4JUVsstn6anQyedUTqcI9SWz7
84S5QRCE9mMtaZGx5LdHoq5dsl5RZRYJqzR9ytq9R/NV5vo0DaLNOLosiP4U6p6n
aFVuHz1WDVXoupSIuSFrtTjVl2Qgo0zM1rlS8Yk3UL8491coVcbipyO0AcX4r+08
5CIYarEPgwNu3nvsCH7hhxt6EvaB6nXJM1CHOKWTpRavEwgdM3aLeu/8XOvtNDNs
ijKnwowo6rF/+qYiGwpSojC1CUeMLp7dkuRYoL7OcPqkTOyeoAseXnIVM8g0r/yA
yqbaaSfiWb+VwwcF++HUslgVmBL8654SZ6mkqTrim1yVfSCAsZUdP/ELbk9+EQcI
MaTytM1hOrAXYNz6nqDXmnf77s+rN2sulokL8s60/PlGccca9GmCsU1kr10yvIwM
7Qr199UpRDXMz8PvX5XKlTX7lDqv0g3yw/Y6VaZ04dnhCVG2qwOc3IJljqtNbNS1
zUaoHdYpSIugA+KzqWByv2VF2SqssyQrEe9AA3giUSsQYX/wlUxDfzZG2ICYNZ7Y
jXXBJixPjoZXbiQ8qdb47YRd3Xq5TqhO082eDCQzuNNGi54be1dZH7rGpdWW1205
MmNqiw6AQc3UzNI4zV5tUKifkBQaI4sO
-----END ENCRYPTED PRIVATE KEY-----"""

# Load environment variables
load_dotenv()

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "transactions"]

# -------- 2MA --------

PASSPHRASE = os.getenv("PASSPHRASE")

# -------- Python Callables --------
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    local_files = {}
    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []
        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")

    private_key = serialization.load_pem_private_key(
    data=PRIVATE_KEY_CONTENT.encode(),
    password=PASSPHRASE.encode() if PASSPHRASE else None,
)
    if not local_files:
        print("No files found in MinIO.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        private_key=private_key,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    # === CRITICAL: Explicitly set current DB and Schema ===
    cur.execute(f"USE DATABASE {SNOWFLAKE_DB};")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA};")

    print(f"Current schema set to: {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}")

    for table, files in local_files.items():
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        # Upload each file to the table's internal stage
        for f in files:
            # Use fully qualified table stage for safety
            put_sql = f"PUT 'file://{f}' @%{table} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
            cur.execute(put_sql)
            print(f"Uploaded {f} -> @{table} stage")

        # Load data with COPY INTO
        copy_sql = f"""
        COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table}
        FROM @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.%{table}
        FILE_FORMAT = (TYPE = PARQUET)
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE;   -- Optional: delete files after successful load
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {table}")

        # Optional: Clean up the table stage
        cur.execute(f"REMOVE @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.%{table};")

    cur.close()
    conn.close()

# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables",
    schedule_interval="0 0 1 */1 *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2