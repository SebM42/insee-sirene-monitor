from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import TimestampType, DateType
from delta.tables import DeltaTable
from utils.config import (
    BRONZE_TABLE, SILVER_TABLE,
    HISTORIZED_COLS, PROJECT_ONLY_HISTORIZED_COLS,
    PROJECT_HISTORIZED_COLS, FIXED_COLS, SILVER_COLS
)
from utils.delta import get_spark
from utils.pipeline_state import set_pipeline_halted


def read_batch(batch_date: str):
    """Read a Bronze batch for the given batch_date, extract the active period
    from periodesEtablissement (dateFin IS NULL), flatten all relevant columns,
    deduplicate on siret, and assert uniqueness before returning."""

    spark = get_spark()
    sdf = spark.table(BRONZE_TABLE).filter(F.col("batch_date") == batch_date)

    sdf_periods = sdf.withColumn(
        "active_period",
        F.filter("periodesEtablissement", lambda p: p["dateFin"].isNull())[0]
    )

    sdf_flat = sdf_periods.select(
        "siret",
        "dateCreationEtablissement",
        "dateDernierTraitementEtablissement",
        "trancheEffectifsEtablissement",
        "etablissementSiege",
        "activitePrincipaleNAF25Etablissement",
        F.col("adresseEtablissement.codePostalEtablissement").alias("codePostalEtablissement"),
        F.col("adresseEtablissement.codeCommuneEtablissement").alias("codeCommuneEtablissement"),
        F.col("adresseEtablissement.libelleCommuneEtablissement").alias("libelleCommuneEtablissement"),
        F.col("adresseEtablissement.coordonneeLambertAbscisseEtablissement").try_cast("double").alias("coordonneeLambertAbscisseEtablissement"),
        F.col("adresseEtablissement.coordonneeLambertOrdonneeEtablissement").try_cast("double").alias("coordonneeLambertOrdonneeEtablissement"),
        F.col("active_period.etatAdministratifEtablissement").alias("etatAdministratifEtablissement"),
        F.col("active_period.activitePrincipaleEtablissement").alias("activitePrincipaleEtablissement"),
        F.col("active_period.caractereEmployeurEtablissement").alias("caractereEmployeurEtablissement"),
        F.col("active_period.dateDebut").cast(DateType()).alias("dateDebut"),
    )

    sdf_flat = sdf_flat.dropDuplicates()

    assert sdf_flat.groupBy("siret").count().filter(F.col("count") > 1).count() == 0, \
        "Integrity error: multiple rows per siret in batch"

    return sdf_flat


def build_to_merge(sdf_batch, batch_date: str):
    """Build the full Silver target DataFrame for all sirets present in the batch.
    Applies three categories of changes:
    - Significant changes (PROJECT_HISTORIZED_COLS): closes the open Silver row
      and inserts a new open row.
    - Fixed column changes (FIXED_COLS): updates the column value across all
      historical Silver rows for the affected siret.
    - New sirets: inserts a new open row directly.
    Returns a DataFrame ready for atomic Delta MERGE into Silver."""

    spark = get_spark()

    sirets = [row.siret for row in sdf_batch.select("siret").collect()]
    sdf_silver = spark.table(SILVER_TABLE).filter(F.col("siret").isin(sirets))

    sdf_open = sdf_silver.filter(F.col("end_at").isNull())

    assert sdf_open.groupBy("siret").count().filter(F.col("count") > 1).count() == 0, \
        "Integrity error: multiple open rows per siret in Silver"

    batch_cols = set(sdf_batch.columns)
    silver_cols = set(sdf_open.columns) - {"siret"}
    common_cols = batch_cols & silver_cols

    for col in common_cols:
        sdf_batch = sdf_batch.withColumnRenamed(col, f"{col}_batch")
        sdf_open = sdf_open.withColumnRenamed(col, f"{col}_silver")

    sdf_joined = sdf_batch.join(sdf_open, on="siret", how="left")

    assert sdf_joined.count() == sdf_batch.count(), \
        f"Join error: sdf_joined has {sdf_joined.count()} rows but sdf_batch has {sdf_batch.count()} rows"

    change_condition = F.lit(False)
    for col in PROJECT_HISTORIZED_COLS:
        change_condition = change_condition | (
            F.col(f"{col}_batch") != F.col(f"{col}_silver")
        )
    sdf_joined = sdf_joined.withColumn("has_significant_change", change_condition)

    fixed_change_condition = F.lit(False)
    for col in FIXED_COLS:
        fixed_change_condition = fixed_change_condition | (
            F.col(f"{col}_batch") != F.col(f"{col}_silver")
        )
    sdf_joined = sdf_joined.withColumn("has_fixed_change", fixed_change_condition)

    sdf_joined = sdf_joined.withColumn(
        "new_end_at",
        F.greatest(
            F.col("dateDebut"),
            F.col("dateDernierTraitementEtablissement").cast(DateType())
        )
    )

    for col in sdf_silver.columns:
        if col != "siret" and col not in common_cols:
            sdf_silver = sdf_silver.withColumnRenamed(col, f"{col}_silver")

    sdf_base = sdf_joined.join(sdf_silver, on="siret", how="left")

    for col in FIXED_COLS:
        sdf_base = sdf_base.withColumn(
            f"{col}_silver",
            F.when(
                F.col("has_fixed_change"),
                F.col(f"{col}_batch")
            ).otherwise(F.col(f"{col}_silver"))
        )

    sdf_base = sdf_base.withColumn(
        "end_at_silver",
        F.when(
            F.col("end_at_silver").isNull() & F.col("has_significant_change"),
            F.col("new_end_at")
        ).otherwise(F.col("end_at_silver"))
    ).withColumn(
        "batch_closed_silver",
        F.when(
            F.col("end_at_silver").isNull() & F.col("has_significant_change"),
            F.lit(batch_date).cast(DateType())
        ).otherwise(F.col("batch_closed_silver"))
    )

    sdf_base_final = sdf_base.select(
        "siret",
        F.col("start_at_silver").alias("start_at"),
        F.col("end_at_silver").alias("end_at"),
        *[F.col(f"{col}_silver").alias(col) for col in PROJECT_HISTORIZED_COLS],
        *[F.col(f"{col}_silver").alias(col) for col in FIXED_COLS],
        F.col("batch_created_silver").alias("batch_created"),
        F.col("batch_closed_silver").alias("batch_closed"),
    ).filter(F.col("start_at").isNotNull())

    sdf_new_rows = sdf_joined.filter(F.col("has_significant_change")).select(
        "siret",
        (F.col("new_end_at") + F.expr("INTERVAL 1 SECOND")).alias("start_at"),
        F.lit(None).cast(DateType()).alias("end_at"),
        *[F.col(f"{col}_batch").alias(col) for col in PROJECT_HISTORIZED_COLS],
        *[F.col(f"{col}_batch").alias(col) for col in FIXED_COLS],
        F.lit(batch_date).cast(DateType()).alias("batch_created"),
        F.lit(None).cast(DateType()).alias("batch_closed"),
    )

    sdf_new_sirets = sdf_joined.filter(F.col("start_at").isNull()).select(
        "siret",
        F.col("new_end_at").alias("start_at"),
        F.lit(None).cast(DateType()).alias("end_at"),
        *[F.col(f"{col}_batch").alias(col) for col in PROJECT_HISTORIZED_COLS],
        *[F.col(f"{col}_batch").alias(col) for col in FIXED_COLS],
        F.lit(batch_date).cast(DateType()).alias("batch_created"),
        F.lit(None).cast(DateType()).alias("batch_closed"),
    )

    sdf_to_merge = sdf_base_final.select(SILVER_COLS) \
        .union(sdf_new_rows.select(SILVER_COLS)) \
        .union(sdf_new_sirets.select(SILVER_COLS))

    check_integrity(sdf_to_merge, sdf_batch, sdf_silver, sdf_joined)

    return sdf_to_merge


def check_integrity(sdf_to_merge, sdf_batch, sdf_silver, sdf_joined) -> None:
    """Run integrity checks on the to_merge DataFrame before committing to Silver:
    - No rows where start_at > end_at
    - Exactly one open row per siret present in the batch
    - Total row count matches expected (silver rows + significant changes + new sirets)"""

    invalid = sdf_to_merge.filter(
        F.col("end_at").isNotNull() & (F.col("start_at") > F.col("end_at"))
    ).count()
    if invalid > 0:
        raise Exception(f"Integrity check failed: {invalid} rows with start_at > end_at")

    sirets_batch = sdf_batch.select("siret").distinct().count()
    sirets_open = sdf_to_merge.filter(F.col("end_at").isNull()).select("siret").distinct().count()
    if sirets_open != sirets_batch:
        raise Exception(f"Integrity check failed: {sirets_batch} sirets in batch but {sirets_open} open rows in to_merge")

    new_sirets = sdf_joined.filter(
        F.col("end_at").isNull() & F.col("start_at").isNull()
    ).count()
    significant_changes = sdf_joined.filter(F.col("has_significant_change")).count()
    fixed_changes = sdf_joined.filter(F.col("has_fixed_change")).count()
    expected = sdf_silver.count() + significant_changes + new_sirets
    actual = sdf_to_merge.count()
    if actual != expected:
        raise Exception(f"Integrity check failed: expected {expected} rows, got {actual}")

    print(f"Integrity check passed: {actual} rows in to_merge ({significant_changes} significant changes, {new_sirets} new sirets, {fixed_changes} fixed col changes)", flush=True)


def merge_to_silver(sdf_to_merge) -> None:
    """Perform an atomic Delta MERGE of the to_merge DataFrame into the Silver table,
    matching on siret + start_at. Updates matched rows and inserts unmatched rows."""

    spark = get_spark()
    silver = DeltaTable.forName(spark, SILVER_TABLE)
    silver.alias("silver").merge(
        sdf_to_merge.alias("updates"),
        "silver.siret = updates.siret AND silver.start_at = updates.start_at"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()


def delete_bronze_batch(batch_date: str) -> None:
    """Delete all rows for the given batch_date from the Bronze Delta table.
    This operation is atomic — either all rows are deleted or none are."""

    DeltaTable.forName(get_spark(), BRONZE_TABLE).delete(F.col("batch_date") == batch_date)


def run_bronze_to_silver() -> None:
    """Main entry point for the Bronze→Silver transformation. Retrieves all
    pending batch dates from Bronze in chronological order and processes each
    one sequentially. On any failure, sets the pipeline circuit breaker and
    exits without retry."""

    try:
        spark = get_spark()
        batch_dates = [
            row.batch_date
            for row in spark.table(BRONZE_TABLE)
                .select("batch_date").distinct()
                .orderBy("batch_date")
                .collect()
        ]
    except Exception as e:
        set_pipeline_halted(str(e))
        print(f"Pipeline halted: {e}", flush=True)
        return

    for batch_date in batch_dates:
        print(f"Processing batch {batch_date}...", flush=True)
        try:
            sdf_batch = read_batch(batch_date)
            print(f"Batch read: {sdf_batch.count()} rows", flush=True)
            sdf_to_merge = build_to_merge(sdf_batch, batch_date)
            print("to_merge built, merging...", flush=True)
            merge_to_silver(sdf_to_merge)
            delete_bronze_batch(batch_date)
            print(f"Batch {batch_date} processed and deleted from Bronze.", flush=True)
        except Exception as e:
            set_pipeline_halted(str(e))
            print(f"Pipeline halted: {e}", flush=True)
            return