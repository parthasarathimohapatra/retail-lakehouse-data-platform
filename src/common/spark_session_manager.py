""" Create and manage the SparkSession used by the retail platform."""
from pyspark.sql import SparkSession

def create_spark_session(
        application_name: str = "RetailLakehouseDataPlatform",
) -> SparkSession:
    """Create a local SparkSession for development and testing."""

    return (
        SparkSession.builder
        .appName(application_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )