from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient


def get_spark(app_name: str = "Gaming") -> SparkSession:
    """
    Creates and returns a SparkSession configured for S3 (MinIO) and Hive Metastore.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Configured Spark session.
    """
    w = WorkspaceClient()
    dbutils = w.dbutils
    storage_key = dbutils.secrets.get("kv-secrets", "storage-access-key")
    spark: SparkSession = (
        SparkSession.builder
        .appName(app_name) # type: ignore
        .enableHiveSupport()
        .getOrCreate()
    )
    storage_account = spark.conf.get("spark.storage.account.name")

    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        storage_key
    )
    return spark

def get_storage_path(
        spark: SparkSession,
        container_var: str,
        storage_var: str="storage.account.name"
    ):
    bronze_container = spark.conf.get(f"spark.{container_var}")
    storage_account = spark.conf.get(f"spark.{storage_var}")
    storage_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net"
    return storage_path
