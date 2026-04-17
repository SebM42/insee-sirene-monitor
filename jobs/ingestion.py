from datetime import datetime
from databricks.sdk import WorkspaceClient
from delta.tables import DeltaTable
from utils.config import SILVER_TABLE, BRONZE_SILVER_WORKFLOW_ID
from utils.delta import get_spark
from ingestion.first_fetch import run_first_fetch
from ingestion.fetch import run_fetch

def trigger_bronze_silver() -> None:
    w = WorkspaceClient()
    w.jobs.run_now(job_id=BRONZE_SILVER_WORKFLOW_ID)
    print("Bronze→Silver workflow triggered.", flush=True)

def main(dbutils) -> None:
    spark = get_spark()
    batch_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    if not DeltaTable.isDeltaTable(spark, SILVER_TABLE):
        run_first_fetch(batch_date)
    run_fetch(dbutils, batch_date)
    trigger_bronze_silver()

if __name__ == "__main__":
    main(dbutils)