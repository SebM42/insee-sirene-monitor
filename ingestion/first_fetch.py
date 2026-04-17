import zipfile
import urllib.request
import urllib.error
import os
import time
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
    SILVER_COLS, SILVER_COMMENTS,SIRENE_INTERMEDIATE_SCHEMA
)
from utils.delta import get_spark
from utils.pipeline_state import set_insee_delta_cursor


STOCK_RAW_PATH = f"/Volumes/{VOLUME_STOCK.replace('.','/')}/raw/"
STOCK_ZIP_PATH = f"{STOCK_RAW_PATH}sirene_stock.zip"
STOCK_CSV_PATH = f"{STOCK_RAW_PATH}sirene_stock.csv"
STOCK_HISTORY_ZIP_PATH = f"{STOCK_RAW_PATH}sirene_history_stock.zip"
STOCK_HISTORY_CSV_PATH = f"{STOCK_RAW_PATH}sirene_history_stock.csv"


def setup() -> None:
    spark = get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {VOLUME_PROJECT}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_STOCK}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_STATE}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_BRONZE_STAGING}")
    os.makedirs(STOCK_RAW_PATH, exist_ok=True)
    os.makedirs(STOCK_FILTERED_BASE_PARQUET_DIR, exist_ok=True)
    os.makedirs(STOCK_FILTERED_HISTORY_PARQUET_DIR, exist_ok=True)


def download_and_extract(url: str, zip_path: str, csv_path: str) -> None:
    if not os.path.exists(zip_path):
        try:
            print(f"Downloading file from {url} ...", flush=True)
            urllib.request.urlretrieve(url, zip_path)
            print(f"Download complete.")
        except urllib.error.HTTPError as e:
            raise Exception(f"HTTP error {e.code} when downloading file")
    else:
        print(f"File already donwloaded.")
    
    if not os.path.exists(csv_path):
        print('Unzipping...', flush=True)
        with zipfile.ZipFile(zip_path) as z:
            csv_filename = [f for f in z.namelist() if f.endswith(".csv")][0]
            z.extract(csv_filename, STOCK_RAW_PATH)
        os.rename(f"{STOCK_RAW_PATH}{csv_filename}", csv_path)
        print('Unzipping complete.')
    else:
        print(f"File already unzipped.")


def filter_stock_and_write_to_volume() -> None:
    spark = get_spark()
    print("Filtering stock file...", flush=True)
    sdf = spark.read.csv(STOCK_CSV_PATH, header=True, sep=",", inferSchema=False)
    sdf = sdf.select(
        *[F.col(field.name).try_cast(field.dataType).alias(field.name) for field in SIRENE_INTERMEDIATE_SCHEMA.fields]
    )
    sdf = sdf.dropna(subset=["codeCommuneEtablissement"])
    sdf = sdf.filter(F.col("codeCommuneEtablissement").substr(1, 2).isin(FILTERED_DEPARTMENTS))
    print("Filtering complete.")
    print(f"Saving to {STOCK_FILTERED_BASE_PARQUET_DIR} ...", flush=True)
    sdf.write.mode("overwrite").parquet(STOCK_FILTERED_BASE_PARQUET_DIR)
    print("Saving complete.")

def filter_history_and_write_to_volume() -> None:
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
    print("History filtering complete.")
    print(f"Saving history to {STOCK_FILTERED_HISTORY_PARQUET_DIR} ...", flush=True)
    sdf.write.mode("overwrite").parquet(STOCK_FILTERED_HISTORY_PARQUET_DIR)
    print("Saving complete.", flush=True)
    

def to_silver_schema(sdf_base, sdf_history, batch_date: str):
    # colonnes non historisées à récupérer depuis le stock
    non_hist_cols = ["siret"] + NON_HISTORIZED_COLS + ["dateCreationEtablissement", "dateDernierTraitementEtablissement"]

    # cap date — on ne peut pas avoir de dates supérieures à max(dateDernierTraitementEtablissement)
    max_date = sdf_base.agg(F.max("dateDernierTraitementEtablissement")).collect()[0][0]
    max_date_lit = F.lit(max_date).cast("date")

    # sirets à période unique — depuis le stock directement
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

    # sirets multi-périodes — depuis l'historique + join stock pour les colonnes non historisées
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

    # sanity check
    expected = sdf_base.agg(F.sum(F.col("nombrePeriodesEtablissement").cast("int"))).collect()[0][0]
    actual = sdf_silver.count()
    if actual != expected:
        raise Exception(f"Silver row count mismatch: expected {expected}, got {actual}")
    print(f"Sanity check passed: {actual} rows in Silver.")

    insee_delta_cursor = sdf_base.agg(F.max("dateDernierTraitementEtablissement")).collect()[0][0]
    set_insee_delta_cursor(insee_delta_cursor.isoformat())

    return sdf_silver


def write_to_silver(batch_date: str) -> None:
    spark = get_spark()
    print("Reading parquet files from Volume...", flush=True)
    sdf_base = spark.read.parquet(STOCK_FILTERED_BASE_PARQUET_DIR)
    sdf_history = spark.read.parquet(STOCK_FILTERED_HISTORY_PARQUET_DIR)
    print("Applying Silver schema transformations...", flush=True)
    sdf = to_silver_schema(sdf_base, sdf_history, batch_date)
    print("Writing to Silver table...", flush=True)
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
    print("Silver table written.")

def add_silver_comments() -> None:
    spark = get_spark()
    for name, comment in SILVER_COMMENTS.items():
        spark.sql(f"ALTER TABLE {SILVER_TABLE} ALTER COLUMN {name} COMMENT '{comment.replace(chr(39), chr(39)*2)}'")

def cleanup() -> None:
    spark = get_spark()
    spark.sql(f"DROP VOLUME IF EXISTS {VOLUME_STOCK}")


def run_first_fetch(batch_date: str) -> None:
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