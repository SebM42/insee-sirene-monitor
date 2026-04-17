from databricks.sdk import WorkspaceClient
from utils.config import SILVER_GOLD_WORKFLOW_ID
from utils.pipeline_state import is_pipeline_halted
from transform.bronze_to_silver import run_bronze_to_silver

def trigger_silver_gold() -> None:
    w = WorkspaceClient()
    w.jobs.run_now(job_id=SILVER_GOLD_WORKFLOW_ID)
    print("Silver→Gold workflow triggered.", flush=True)

def main() -> None:
    if is_pipeline_halted():
        print("Pipeline is halted, skipping Bronze→Silver.", flush=True)
        return
    run_bronze_to_silver()
    trigger_silver_gold()

if __name__ == "__main__":
    main()