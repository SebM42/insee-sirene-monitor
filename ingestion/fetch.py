import requests
import json
import os
import shutil
import time
import math
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from utils.config import (
    INSEE_API_ENDPOINT, FILTERED_DEPARTMENTS,
    BRONZE_TABLE, BRONZE_STAGING_DIR
)
from utils.delta import get_spark
from utils.pipeline_state import get_insee_delta_cursor, set_insee_delta_cursor
from utils.storage import get_insee_api_key

def fetch_paginated(api_key: str, q: str, output_path: str, endlog: str) -> None:
    debut = 0
    while True:
        response = requests.get(
            INSEE_API_ENDPOINT,
            headers={"X-INSEE-Api-Key-Integration": api_key},
            params={"q": q, "nombre": 1000, "debut": debut}
        )
        if response.status_code == 429:
            reset_ts = int(response.headers["X-Rate-Limit-Reset"]) / 1000
            sleep_time = max(0, reset_ts - time.time()) + 1
            print(f"429 Too Many Requests, sleeping {sleep_time:.1f}s...", flush=True)
            time.sleep(sleep_time)
            continue  # retry la même requête sans incrémenter debut
        response.raise_for_status()
        data = response.json()
        total = data["header"]["total"]
        with open(output_path, "a") as f:
            for e in data["etablissements"]:
                f.write(json.dumps(e) + "\n")
        debut += 1000
        if debut >= total:
            break
        remaining = int(response.headers["X-Rate-Limit-Remaining"])
        if remaining == 0:
            reset_ts = int(response.headers["X-Rate-Limit-Reset"]) / 1000
            sleep_time = max(0, reset_ts - time.time()) + 1
            print(f"Rate limit reached, sleeping {sleep_time:.1f}s ...", flush=True)
            time.sleep(sleep_time)
    print(endlog, flush=True)


def fetch_dept_in_time_range(api_key: str, start: str, end: str, output_path: str, dept: str) -> None:
    q_base = f"codeCommuneEtablissement:{dept}*"
    q = f"dateDernierTraitementEtablissement:[{start} TO {end}] AND {q_base}"
    
    print(f"Querying for total number of results for dept {dept} from {start} TO {end} ...", flush=True)
    response = requests.get(
        INSEE_API_ENDPOINT,
        headers={"X-INSEE-Api-Key-Integration": api_key},
        params={"q": q, "nombre": 1}
    )

    if response.status_code == 429:
        reset_ts = int(response.headers["X-Rate-Limit-Reset"]) / 1000
        sleep_time = max(0, reset_ts - time.time()) + 1
        print(f"429 Too Many Requests, sleeping {sleep_time:.1f}s...", flush=True)
        time.sleep(sleep_time)
        return fetch_dept_in_time_range(api_key, start, end, output_path, dept)  # retry récursif

    data = response.json()
    if data["header"]["statut"] == 404 and data["header"]["message"].startswith("Aucun élément trouvé"):
        print(f"No results found, skipping.", flush=True)
        return

    response.raise_for_status()
    total = response.json()["header"]["total"]
    print(f"Dept {dept} from {start} TO {end}: {total} results", flush=True)

    if total > 10000:
        n_chunks = math.ceil(total / 10000) + 1
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.now() if end == "*" else datetime.fromisoformat(end)
        delta = (end_dt - start_dt) / n_chunks
        print(f"Dept {dept} from {start} TO {end} - results > 10k : splitting into {n_chunks} chunks", flush=True)
        for i in range(n_chunks):
            chunk_start = (start_dt + delta * i).strftime("%Y-%m-%dT%H:%M:%S")
            chunk_end = "*" if i + 1 == n_chunks else (start_dt + delta * (i + 1)).strftime("%Y-%m-%dT%H:%M:%S")
            fetch_dept_in_time_range(api_key, chunk_start, chunk_end, output_path, dept)
    else:
        print(f"Fetching all for dept {dept} from {start} TO {end} ...", flush=True)
        fetch_paginated(api_key, q, output_path, f"Dept {dept} appended to {output_path}")


def fetch_data_from_insee(dbutils, tmp_path) -> int:
    api_key = get_insee_api_key(dbutils)
    cursor = get_insee_delta_cursor()

    if cursor is None:
        raise Exception("No cursor found in pipeline state — run first_fetch first")

    print(f"Fetching delta since {cursor}...", flush=True)
    for dept in FILTERED_DEPARTMENTS:
        fetch_dept_in_time_range(api_key, cursor, "*", tmp_path, dept)
    
    if not os.path.exists(tmp_path):
        print(f"Total fetched: 0", flush=True)
        
        return 0
    
    with open(tmp_path, "r") as f:
        count = sum(1 for _ in f)
    print(f"Total fetched: {count}", flush=True)
    
    return count

def copy_data_from_tmp_to_volume(tmp_path: str, volume_path: str) -> None:
    shutil.copy(tmp_path, volume_path)
    print(f"Copied {tmp_path} to {volume_path}")


def write_batch_to_bronze(batch_date: str, output_path:str) -> str:
    print(f"Writing in bronze from {output_path} ...")
    spark = get_spark()
    sdf = spark.read.json(output_path)
    sdf = sdf.withColumn("batch_date", F.lit(batch_date).cast(DateType()))
    sdf.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(BRONZE_TABLE)
    new_cursor = sdf.agg(F.max("dateDernierTraitementEtablissement")).collect()[0][0]
    print("Bronze write complete.")

    new_cursor = (datetime.fromisoformat(str(new_cursor)) + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")
    return new_cursor


def clean_up_staging_rep() -> None:
    for f in os.listdir(BRONZE_STAGING_DIR):
        os.remove(f"{BRONZE_STAGING_DIR}/{f}")
    print("Staging rep cleanup complete.")


def run_fetch(dbutils, batch_date: str) -> None:
    volume_path = f"{BRONZE_STAGING_DIR}/{batch_date}.json"
    tmp_path = f"/tmp/{batch_date}.json"
    
    results = fetch_data_from_insee(dbutils, tmp_path)
    if results > 0:
        copy_data_from_tmp_to_volume(tmp_path, volume_path)
        new_cursor  = write_batch_to_bronze(batch_date, volume_path)
        set_insee_delta_cursor(new_cursor)
    else :
        print("No new data to fetch. Skipping.")
    clean_up_staging_rep()
    