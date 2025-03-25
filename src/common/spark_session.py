import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "Gaming") -> SparkSession:
    """
    Creates and returns a SparkSession configured for S3 (MinIO) and Hive Metastore.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder
        .appName(app_name) # type: ignore
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.3.101:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", "s3a://gaming/")
        .enableHiveSupport()
        .getOrCreate()
    )
