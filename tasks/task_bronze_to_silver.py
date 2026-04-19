from databricks.sdk import WorkspaceClient
from utils.pipeline_state import is_pipeline_halted
from transform.bronze_to_silver import run_bronze_to_silver


def trigger_silver_gold(dbutils) -> None:
    """Trigger the Silver→Gold Databricks job via the Databricks SDK,
    retrieving the job ID from the Databricks secret vault."""

    w = WorkspaceClient()
    SILVER_GOLD_JOB_ID = int(dbutils.secrets.get(scope="databricks-credentials", key="silver-gold-job-id"))
    w.jobs.run_now(job_id=SILVER_GOLD_JOB_ID)
    print("Silver→Gold job triggered.", flush=True)


def main(dbutils) -> None:
    """Main entry point for the Bronze→Silver task. Exits early if the
    pipeline circuit breaker is set. On success, triggers the Silver→Gold job."""

    if is_pipeline_halted():
        print("Pipeline is halted, skipping Bronze→Silver.", flush=True)
        return
    run_bronze_to_silver()
    trigger_silver_gold(dbutils)


if __name__ == "__main__":
    main(dbutils)