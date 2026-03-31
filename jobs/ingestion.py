from datetime import datetime
from delta.tables import DeltaTable
from utils.config import SILVER_TABLE, DATABRICKS_SCOPE, BRONZE_SILVER_WORKFLOW_ID
from utils.delta import get_spark
from ingestion.first_fetch import run_first_fetch
from ingestion.fetch import run_fetch
import requests

def trigger_bronze_silver(dbutils) -> None:
    host = dbutils.secrets.get(scope=DATABRICKS_SCOPE, key="host")
    token = dbutils.secrets.get(scope=DATABRICKS_SCOPE, key="token")
    response = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {token}"},
        json={"job_id": BRONZE_SILVER_WORKFLOW_ID}
    )
    response.raise_for_status()
    print("Bronze→Silver workflow triggered.", flush=True)

def main(dbutils) -> None:
    spark = get_spark()
    batch_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    if not DeltaTable.isDeltaTable(spark, SILVER_TABLE):
        run_first_fetch(batch_date)
    run_fetch(dbutils, batch_date)
    trigger_bronze_silver(dbutils)

if __name__ == "__main__":
    main(dbutils)