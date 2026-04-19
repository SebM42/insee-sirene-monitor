from databricks.sdk import WorkspaceClient
from utils.pipeline_state import reset_pipeline_halt


def trigger_bronze_silver(dbutils) -> None:
    """Trigger the Bronze→Silver Databricks job via the Databricks SDK,
    retrieving the job ID from the Databricks secret vault."""

    w = WorkspaceClient()
    BRONZE_SILVER_JOB_ID = int(dbutils.secrets.get(scope="databricks-credentials", key="bronze-silver-job-id"))
    w.jobs.run_now(job_id=BRONZE_SILVER_JOB_ID)
    print("Bronze→Silver job triggered.", flush=True)


def main(dbutils) -> None:
    """Main entry point for the pipeline reset task. Clears the circuit breaker
    flag and immediately triggers the Bronze→Silver job to resume pipeline processing."""

    reset_pipeline_halt()
    print("Pipeline halt reset.", flush=True)
    trigger_bronze_silver(dbutils)


if __name__ == "__main__":
    main(dbutils)