import dagster as dg
import os
from serpapi import GoogleSearch # type: ignore
from google.cloud import bigquery
import json
from datetime import datetime, timezone
from typing import Dict, List

def _map_row(r: dict) -> dict:
    jid = r.get("job_id")
    if not jid:
        raise ValueError("Missing job_id in source row; cannot load into REQUIRED column.")
    return {
        "job_id": jid,
        "title": r.get("title"),
        "company_name": r.get("company_name"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "job_data": r,  # full payload goes here
    }


@dg.asset
def get_jobs():
    api_key = os.getenv("SERPAPI_KEY")
    job_title = os.getenv("JOB_TITLE")
    location = os.getenv("LOCATION")

    # BQ destination
    project   = os.getenv("BQ_PROJ")
    dataset   = os.getenv("RAW_DATASET")
    table     = os.getenv("SERPAPI_TABLE")

    params = {
        "engine": "google_jobs",
        "q": f"{job_title} {location}",
        "hl": "en",
        "api_key": api_key,
    }

    max_jobs = 500
    max_pages = 50  # SerpApi returns up to 10 results per page
    seen_ids: Dict[str, dict] = {}
    next_page_token = None
    page = 0

    # Fetch pages until we reach max_jobs unique IDs or exhaust pagination
    while len(seen_ids) < max_jobs and page < max_pages:
        page_params = dict(params)
        if next_page_token:
            page_params["next_page_token"] = next_page_token

        result = GoogleSearch(page_params).get_dict()
        page_jobs = result.get("jobs_results", []) or []

        # Deduplicate by job_id while preserving first occurrence
        for job in page_jobs:
            jid = job.get("job_id")
            if jid and jid not in seen_ids:
                seen_ids[jid] = job
                if len(seen_ids) >= max_jobs:
                    break

        next_page_token = result.get("serpapi_pagination", {}).get("next_page_token")
        if not next_page_token:
            break

        page += 1

    jobs_json = list(seen_ids.values())

    with open("jobs.json", "w", encoding="utf-8") as f:
        json.dump(jobs_json, f, ensure_ascii=False, indent=2)
    
    print(jobs_json)

    if not jobs_json:
        return {"inserted_rows": 0, "table": f"{project}.{dataset}.{table}", "note": "No jobs_results returned"}
    
    # Validate that every job has a job_id since the column is REQUIRED
    missing = [i for i, r in enumerate(jobs_json) if not r.get("job_id")]
    if missing:
        raise ValueError(f"{len(missing)} job(s) missing job_id; cannot load into REQUIRED column. Offending indices: {missing[:10]} (showing up to 10).")
    
    rows = [_map_row(r) for r in jobs_json]
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    
    # Ensure table exists with desired schema & partitioning
    schema = [
        bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("job_data", "JSON"),
    ]

    # Create table if it doesn't exist (no partitioning)
    try:
        client.get_table(table_id)
        table_exists = True
    except Exception:
        table_obj = bigquery.Table(table_id, schema=schema)
        table_obj.clustering_fields = ["job_id"]  # optional, for faster lookups
        client.create_table(table_obj)
        table_exists = False

    # Load data
    if table_exists:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    else:
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
    load_job.result()

    load_result = {"inserted_rows": len(rows), "table": table_id}
    print(load_result)
    return load_result

daily_schedule = dg.ScheduleDefinition(
    name="daily_refresh",
    cron_schedule="0 6 * * *",  # Runs at 6:00 daily
    target=[get_jobs],
)
