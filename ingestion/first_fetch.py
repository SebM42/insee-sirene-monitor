import zipfile
import urllib.request
import urllib.error
import os
import json
import requests
import shutil
import pandas as pd
from pyspark.sql import functions as F
from utils.config import (
    STOCK_FILE_URL, STOCK_HISTORY_FILE_URL,
    FILTERED_DEPARTMENTS, SILVER_TABLE,
    VOLUME_STOCK, VOLUME_PROJECT, VOLUME_STATE, VOLUME_BRONZE_STAGING,
    STOCK_FILTERED_BASE_PARQUET_DIR,
    STOCK_FILTERED_HISTORY_PARQUET_DIR,
    INSEE_API_ENDPOINT, HISTORIZED_COLS, NON_HISTORIZED_COLS,
    SILVER_COLS, SILVER_COMMENTS, SIRENE_INTERMEDIATE_SCHEMA
)
from utils.delta import get_spark
from utils.pipeline_state import set_insee_delta_cursor


STOCK_RAW_PATH = f"/Volumes/{VOLUME_STOCK.replace('.','/')}/raw/"
STOCK_ZIP_PATH = f"{STOCK_RAW_PATH}sirene_stock.zip"
STOCK_CSV_PATH = f"{STOCK_RAW_PATH}sirene_stock.csv"
STOCK_HISTORY_ZIP_PATH = f"{STOCK_RAW_PATH}sirene_history_stock.zip"
STOCK_HISTORY_CSV_PATH = f"{STOCK_RAW_PATH}sirene_history_stock.csv"


def setup() -> None:
    """Create the Unity Catalog schema, volumes, and local directories
    required by the first_fetch pipeline. Idempotent — safe to run multiple times."""

    spark = get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {VOLUME_PROJECT}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_STOCK}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_STATE}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_BRONZE_STAGING}")
    os.makedirs(STOCK_RAW_PATH, exist_ok=True)
    os.makedirs(STOCK_FILTERED_BASE_PARQUET_DIR, exist_ok=True)
    os.makedirs(STOCK_FILTERED_HISTORY_PARQUET_DIR, exist_ok=True)


def download_and_extract(url: str, zip_path: str, csv_path: str) -> None:
    """Download a zip file from the given URL to zip_path, then extract
    the first CSV found inside to csv_path. Skips download or extraction
    if the target file already exists."""

    if not os.path.exists(zip_path):
        try:
            print(f"Downloading file from {url} ...", flush=True)
            urllib.request.urlretrieve(url, zip_path)
            print("Download complete.", flush=True)
        except urllib.error.HTTPError as e:
            raise Exception(f"HTTP error {e.code} when downloading file")
    else:
        print("File already downloaded.", flush=True)

    if not os.path.exists(csv_path):
        print("Unzipping...", flush=True)
        with zipfile.ZipFile(zip_path) as z:
            csv_filename = [f for f in z.namelist() if f.endswith(".csv")][0]
            z.extract(csv_filename, STOCK_RAW_PATH)
        os.rename(f"{STOCK_RAW_PATH}{csv_filename}", csv_path)
        print("Unzipping complete.", flush=True)
    else:
        print("File already unzipped.", flush=True)


def filter_stock_and_write_to_volume() -> None:
    """Read the full SIRENE stock CSV, select and cast columns defined in
    SIRENE_INTERMEDIATE_SCHEMA, filter on AURA departments, and write the
    result as Parquet to the stock Volume."""

    spark = get_spark()
    print("Filtering stock file...", flush=True)
    sdf = spark.read.csv(STOCK_CSV_PATH, header=True, sep=",", inferSchema=False)
    sdf = sdf.select(
        *[F.col(field.name).try_cast(field.dataType).alias(field.name) for field in SIRENE_INTERMEDIATE_SCHEMA.fields]
    )
    sdf = sdf.dropna(subset=["codeCommuneEtablissement"])
    sdf = sdf.filter(F.col("codeCommuneEtablissement").substr(1, 2).isin(FILTERED_DEPARTMENTS))
    print("Filtering complete.", flush=True)
    print(f"Saving to {STOCK_FILTERED_BASE_PARQUET_DIR} ...", flush=True)
    sdf.write.mode("overwrite").parquet(STOCK_FILTERED_BASE_PARQUET_DIR)
    print("Saving complete.", flush=True)


def filter_history_and_write_to_volume() -> None:
    """Read the INSEE historical stock CSV, join on sirets with multiple periods
    from the filtered base stock, select only the historized columns, and write
    the result as Parquet to the history Volume."""

    spark = get_spark()
    print("Loading filtered sirets with multiple periods...", flush=True)
    sirets = spark.read.parquet(STOCK_FILTERED_BASE_PARQUET_DIR) \
        .filter(F.col("nombrePeriodesEtablissement").cast("int") > 1) \
        .select("siret")
    print("Filtering history file...", flush=True)
    sdf = spark.read.csv(STOCK_HISTORY_CSV_PATH, header=True, sep=",", inferSchema=False)
    sdf = sdf.join(sirets, on="siret", how="inner")
    sdf = sdf.select(
        "siret",
        "dateDebut",
        "dateFin",
        "etatAdministratifEtablissement",
        "activitePrincipaleEtablissement",
        "caractereEmployeurEtablissement"
    )
    print("History filtering complete.", flush=True)
    print(f"Saving history to {STOCK_FILTERED_HISTORY_PARQUET_DIR} ...", flush=True)
    sdf.write.mode("overwrite").parquet(STOCK_FILTERED_HISTORY_PARQUET_DIR)
    print("Saving complete.", flush=True)


def to_silver_schema(sdf_base, sdf_history, batch_date: str):
    """Build the Silver DataFrame from the filtered stock and history Parquet files.
    Single-period establishments are taken directly from the stock. Multi-period
    establishments are reconstructed from the history file, joined with non-historized
    columns from the stock. All dates are capped at MAX(dateDernierTraitementEtablissement)
    to prevent future start_at values. Runs a sanity check on row count before returning."""

    non_hist_cols = ["siret"] + NON_HISTORIZED_COLS + ["dateCreationEtablissement", "dateDernierTraitementEtablissement"]

    max_date = sdf_base.agg(F.max("dateDernierTraitementEtablissement")).collect()[0][0]
    max_date_lit = F.lit(max_date).cast("date")

    sdf_single = sdf_base.filter(F.col("nombrePeriodesEtablissement").cast("int") == 1) \
        .select(
            "siret",
            F.least(
                F.greatest(F.col("dateDebut").cast("date"), F.col("dateDernierTraitementEtablissement").cast("date")),
                max_date_lit
            ).alias("start_at"),
            F.lit(None).cast("date").alias("end_at"),
            "dateCreationEtablissement",
            *HISTORIZED_COLS,
            *NON_HISTORIZED_COLS,
            F.lit(batch_date).cast("date").alias("batch_created"),
            F.lit(None).cast("date").alias("batch_closed"),
        )

    sdf_non_hist = sdf_base.select(non_hist_cols)
    sdf_multi = sdf_history.join(sdf_non_hist, on="siret", how="inner") \
        .select(
            "siret",
            F.least(F.col("dateDebut").cast("date"), max_date_lit).alias("start_at"),
            F.when(F.col("dateFin").isNotNull(), F.least(F.col("dateFin").cast("date"), max_date_lit)).otherwise(F.lit(None).cast("date")).alias("end_at"),
            "dateCreationEtablissement",
            *HISTORIZED_COLS,
            *NON_HISTORIZED_COLS,
            F.lit(batch_date).cast("date").alias("batch_created"),
            F.when(F.col("dateFin").isNotNull(), F.lit(batch_date).cast("date")).otherwise(F.lit(None).cast("date")).alias("batch_closed")
        )

    sdf_silver = sdf_single.union(sdf_multi)

    expected = sdf_base.agg(F.sum(F.col("nombrePeriodesEtablissement").cast("int"))).collect()[0][0]
    actual = sdf_silver.count()
    if actual != expected:
        raise Exception(f"Silver row count mismatch: expected {expected}, got {actual}")
    print(f"Sanity check passed: {actual} rows in Silver.", flush=True)

    insee_delta_cursor = sdf_base.agg(F.max("dateDernierTraitementEtablissement")).collect()[0][0]
    set_insee_delta_cursor(insee_delta_cursor.isoformat())

    return sdf_silver


def write_to_silver(batch_date: str) -> None:
    """Read filtered stock and history Parquet files, apply Silver schema
    transformations, and write the result to the Silver Delta table."""

    spark = get_spark()
    print("Reading parquet files from Volume...", flush=True)
    sdf_base = spark.read.parquet(STOCK_FILTERED_BASE_PARQUET_DIR)
    sdf_history = spark.read.parquet(STOCK_FILTERED_HISTORY_PARQUET_DIR)
    print("Applying Silver schema transformations...", flush=True)
    sdf = to_silver_schema(sdf_base, sdf_history, batch_date)
    print("Writing to Silver table...", flush=True)
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
    print("Silver table written.", flush=True)


def add_silver_comments() -> None:
    """Add column-level comments to the Silver Delta table
    using the descriptions defined in SILVER_COMMENTS."""

    spark = get_spark()
    for name, comment in SILVER_COMMENTS.items():
        spark.sql(f"ALTER TABLE {SILVER_TABLE} ALTER COLUMN {name} COMMENT '{comment.replace(chr(39), chr(39)*2)}'")


def cleanup() -> None:
    """Drop the stock Volume after first_fetch is complete.
    The Volume is no longer needed once Silver has been written."""

    spark = get_spark()
    spark.sql(f"DROP VOLUME IF EXISTS {VOLUME_STOCK}")


def run_first_fetch(batch_date: str) -> None:
    """Main entry point for the one-shot pipeline initialization.
    Downloads and filters the SIRENE stock and history files, builds
    the Silver SCD2 table, adds column comments, and cleans up staging volumes."""

    setup()
    if not any(f.endswith(".parquet") for f in os.listdir(STOCK_FILTERED_BASE_PARQUET_DIR)):
        download_and_extract(STOCK_FILE_URL, STOCK_ZIP_PATH, STOCK_CSV_PATH)
        filter_stock_and_write_to_volume()
    if not any(f.endswith(".parquet") for f in os.listdir(STOCK_FILTERED_HISTORY_PARQUET_DIR)):
        download_and_extract(STOCK_HISTORY_FILE_URL, STOCK_HISTORY_ZIP_PATH, STOCK_HISTORY_CSV_PATH)
        filter_history_and_write_to_volume()
    write_to_silver(batch_date)
    add_silver_comments()
    cleanup()