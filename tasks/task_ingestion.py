from datetime import datetime
from databricks.sdk import WorkspaceClient
from delta.tables import DeltaTable
from utils.delta import get_spark
from utils.config import SILVER_TABLE
from ingestion.first_fetch import run_first_fetch
from ingestion.fetch import run_fetch


def trigger_bronze_silver(dbutils) -> None:
    """Trigger the Bronze→Silver Databricks job via the Databricks SDK,
    retrieving the job ID from the Databricks secret vault."""

    w = WorkspaceClient()
    BRONZE_SILVER_JOB_ID = int(dbutils.secrets.get(scope="databricks-credentials", key="bronze-silver-job-id"))
    w.jobs.run_now(job_id=BRONZE_SILVER_JOB_ID)
    print("Bronze→Silver job triggered.", flush=True)


def main(dbutils) -> None:
    """Main entry point for the ingestion task. Runs first_fetch if the Silver
    table does not yet exist, then runs the monthly delta fetch. On success,
    triggers the Bronze→Silver job."""

    spark = get_spark()
    batch_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    silver_exists = spark.catalog.tableExists(SILVER_TABLE)
    if not silver_exists:
        run_first_fetch(batch_date)
    
    run_fetch(dbutils, batch_date)
    trigger_bronze_silver(dbutils)


if __name__ == "__main__":
    main(dbutils)