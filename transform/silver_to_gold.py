import subprocess
import json
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utils.config import GOLD_TABLES, DBT_PROJECT_DIR
from utils.delta import get_spark


def capture_gold_versions() -> dict[str, int]:
    """Capture the current Delta version of each Gold table before running dbt.
    Returns a dict mapping table name to version number, or None if the table
    does not yet exist."""

    spark = get_spark()
    versions = {}
    for table in GOLD_TABLES:
        try:
            versions[table] = DeltaTable.forName(spark, table).history(1).collect()[0]["version"]
        except Exception:
            versions[table] = None
    return versions


def run_dbt() -> None:
    """Run dbt build for the Gold models. Prints stdout and stderr.
    Does not raise on dbt failure — use parse_dbt_failures() to detect errors."""

    result = subprocess.run(
        ["dbt", "build", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
        capture_output=True,
        text=True
    )
    print(result.stdout, flush=True)
    if result.stderr:
        print(result.stderr, flush=True)


def parse_dbt_failures() -> list[str]:
    """Parse the dbt run_results.json file and return a list of failed model names.
    A model is considered failed if its status is 'error' or 'fail'."""

    results_path = f"{DBT_PROJECT_DIR}/target/run_results.json"
    with open(results_path, "r") as f:
        run_results = json.load(f)

    failed_models = []
    for result in run_results["results"]:
        if result["status"] in ("error", "fail"):
            model_name = result["unique_id"].split(".")[-1]
            failed_models.append(model_name)

    return failed_models


def restore_gold_tables(failed_models: list[str], versions: dict[str, int]) -> None:
    """Restore failed Gold tables to their pre-dbt version using Delta time travel.
    If a table did not exist before the dbt run, it is dropped instead."""

    spark = get_spark()
    for model in failed_models:
        table = f"sirene.{model}"
        version = versions.get(table)
        if version is not None:
            spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {version}")
            print(f"Restored {table} to version {version}", flush=True)
        else:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped {table} (did not exist before)", flush=True)


def run_silver_to_gold() -> None:
    """Main entry point for the Silver→Gold transformation. Captures current Gold
    versions, runs dbt build, and rolls back any failed models via Delta time travel.
    Raises on dbt failure after rollback, or raises a critical error if rollback fails."""

    print("Capturing Gold versions...", flush=True)
    versions = capture_gold_versions()

    print("Running dbt build...", flush=True)
    run_dbt()

    failed_models = parse_dbt_failures()
    if failed_models:
        print(f"dbt failures detected: {failed_models}", flush=True)
        try:
            restore_gold_tables(failed_models, versions)
            raise Exception(f"dbt build failed on models: {failed_models} — Gold rolled back successfully")
        except Exception as rollback_error:
            raise Exception(
                f"CRITICAL: dbt build failed AND rollback failed. "
                f"Gold tables may be in inconsistent state. "
                f"Failed models: {failed_models}. "
                f"Rollback error: {rollback_error}"
            )

    print("Silver→Gold complete.", flush=True)