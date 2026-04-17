from databricks.sdk import WorkspaceClient
from utils.config import BRONZE_SILVER_WORKFLOW_ID
from utils.pipeline_state import reset_pipeline_halt

def main() -> None:
    reset_pipeline_halt()
    print("Pipeline reset.", flush=True)
    w = WorkspaceClient()
    w.jobs.run_now(job_id=BRONZE_SILVER_WORKFLOW_ID)
    print("Bronze→Silver workflow triggered.", flush=True)

if __name__ == "__main__":
    main()