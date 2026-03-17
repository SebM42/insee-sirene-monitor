from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utils.config import BRONZE_TABLE, SILVER_TABLE, GOLD_TABLES


def get_spark() -> SparkSession:
    """Retrieve active Spark session."""
    return SparkSession.getActiveSession()


# ═══════════════════════════════════════════════════════
# BRONZE
# ═══════════════════════════════════════════════════════

def get_pending_batches() -> list[str]:
    """Return list of batch dates in Bronze, ordered chronologically."""
    return [
        row.batch_date
        for row in get_spark().table(BRONZE_TABLE)
            .select("batch_date")
            .distinct()
            .orderBy("batch_date")
            .collect()
    ]


def write_to_bronze(df, batch_date: str) -> None:
    """Write a batch to Bronze — MERGE on siret + batch_date for idempotence."""
    bronze = DeltaTable.forName(get_spark(), BRONZE_TABLE)
    bronze.alias("target").merge(
        df.alias("source"),
        "target.siret = source.siret AND target.batch_date = source.batch_date"
    ).whenNotMatchedInsertAll().execute()


def delete_bronze_batch(batch_date: str) -> None:
    """Delete a processed batch from Bronze."""
    DeltaTable.forName(get_spark(), BRONZE_TABLE).delete(
        F.col("batch_date") == batch_date
    )


# ═══════════════════════════════════════════════════════
# SILVER
# ═══════════════════════════════════════════════════════

def get_silver_version() -> int:
    """Capture current Silver table version for potential rollback."""
    return DeltaTable.forName(get_spark(), SILVER_TABLE).history(1).collect()[0]["version"]


def get_last_successful_run_date() -> str | None:
    """Return MAX(batch_date) from Silver — used as delta filter for API calls."""
    return get_spark().table(SILVER_TABLE).agg(F.max("batch_date")).collect()[0][0]


def update_open_records(batch_date: str, sirets: list[str]) -> None:
    """Close open Silver records for establishments present in the batch."""
    DeltaTable.forName(get_spark(), SILVER_TABLE).update(
        condition=(
            F.col("siret").isin(sirets) &
            F.col("end_at").isNull() &
            (F.col("start_at") != batch_date)
        ),
        set={"end_at": F.lit(batch_date)}
    )


def insert_new_records(df) -> None:
    """Insert new Silver records from transformed batch."""
    df.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)


def restore_silver(version: int) -> None:
    """Restore Silver table to a previous version."""
    get_spark().sql("RESTORE TABLE {} TO VERSION AS OF {}".format(SILVER_TABLE, version))


# ═══════════════════════════════════════════════════════
# GOLD
# ═══════════════════════════════════════════════════════

def get_gold_versions() -> dict[str, int]:
    """Capture current versions of all Gold tables for potential rollback."""
    return {
        table: DeltaTable.forName(get_spark(), table).history(1).collect()[0]["version"]
        for table in GOLD_TABLES
    }


def restore_gold_tables(failed_models: list[str], versions: dict[str, int]) -> None:
    """Restore failed Gold tables to their captured versions."""
    for model in failed_models:
        version = versions.get(model)
        if version is not None:
            get_spark().sql(f"RESTORE TABLE {model} TO VERSION AS OF {version}")