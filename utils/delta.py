from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Retrieve active Spark session."""
    return SparkSession.getActiveSession()