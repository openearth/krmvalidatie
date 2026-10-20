"""Lambda handler that downloads the Waterinfo data marked ``DT`` in validatielijst.csv.

Starts when a message is published to the DownloadWaterinfo SNS topic, or when the
scheduled EventBridge rule fires. The content of the message is ignored. What gets
downloaded is decided only by two CSV files in the repository's ``data`` folder,
which the lambda reads from GitHub (raw URL), like the validation lambda does:

* ``validatielijst.csv``: every row whose ``process`` column is ``DT`` is
  downloaded for the period ``startdatum`` - ``einddatum``, with its
  ``grootheid_code`` and ``monstercompartiment_code``, for the locations in
  ``locatiecode``.
* ``waterinfo_location_codes.csv``: translates a ``locatiecode`` of
  validatielijst (for example ``strand_Bergen``) to the Waterinfo location code.

Each DT row produces one file ``<databundelcode>_<YYYYMMDD>.csv`` with the
locations combined, in S3 under the ``downloaded`` prefix. Running the file
directly (``python krm-downloading-validatielijst.py [output_dir]``) writes the
files to a local folder instead, which is handy for testing.
"""

import io
import json
import os
import sys
from datetime import date

import pandas as pd
import rws_waterinfo as rw
import urllib3

# TEMPORARY: development branch, change to "main" once the branch is merged
GITHUB_BASE_URL = os.environ.get(
    "KRM_GITHUB_BASE_URL",
    "https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/waterinfo_downloading/data",
)
BUCKET_NAME = os.environ.get("KRM_BUCKET_NAME", "krm-validatie-data-dev")
S3_PREFIX = os.environ.get("KRM_S3_PREFIX", "downloaded")
MAX_WORKERS = int(os.environ.get("KRM_MAX_WORKERS", "8"))

PROCESS_DOWNLOAD = "DT"
VALIDATIELIJST_COLUMNS = [
    "databundelcode",
    "grootheid_code",
    "monstercompartiment_code",
    "locatiecode",
    "startdatum",
    "einddatum",
    "process",
]
LOCATION_COLUMNS = ["locatiecode", "waterinfo_code"]


def get_data_from_github(filename, required_columns, encoding="windows-1252", delimiter=";"):
    """Fetch a CSV from GitHub raw, the same way github_functions.py does in the validation lambda."""
    url = f"{GITHUB_BASE_URL}/{filename}"
    response = urllib3.PoolManager().request("GET", url)
    if response.status != 200:
        raise RuntimeError(f"Failed to fetch {url}: HTTP {response.status}")

    df = pd.read_csv(io.StringIO(response.data.decode(encoding)), delimiter=delimiter, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"{url} lacks column(s) {missing}. Found: {list(df.columns)}")
    return df


def load_location_codes():
    """Map the locatiecode names of validatielijst to Waterinfo location codes."""
    df = get_data_from_github("waterinfo_location_codes.csv", LOCATION_COLUMNS)
    return dict(zip(df["locatiecode"].str.strip(), df["waterinfo_code"].str.strip()))


def load_rows_to_download():
    """Rows of validatielijst.csv whose process column is DT."""
    df = get_data_from_github("validatielijst.csv", VALIDATIELIJST_COLUMNS)
    return df[df["process"].fillna("").str.strip().str.upper() == PROCESS_DOWNLOAD]


def download_row(row, location_codes):
    """Download all locations of one validatielijst row and combine them into one DataFrame."""
    start = pd.to_datetime(row["startdatum"], dayfirst=True).strftime("%Y-%m-%d")
    end = pd.to_datetime(row["einddatum"], dayfirst=True).strftime("%Y-%m-%d")
    grootheid = row["grootheid_code"].strip()
    compartiment = row["monstercompartiment_code"].strip()

    names = [n.strip() for n in row["locatiecode"].split(";")]
    unknown = [n for n in names if n not in location_codes]
    if unknown:
        raise ValueError(f"No Waterinfo code known for {unknown}, add them to waterinfo_location_codes.csv")

    frames = []
    failures = []
    for name in names:
        loc = location_codes[name]
        params = [[compartiment, "", "", grootheid, loc, start, end]]
        print(f"Downloading {loc} ({start} - {end}, {grootheid}, {compartiment})")
        try:
            data = rw.get_data(params=params, return_df=True, max_workers=MAX_WORKERS)
        except Exception as exc:  # one bad location must not stop the others
            print(f"  failed: {exc}")
            failures.append({"location": loc, "error": str(exc)})
            continue

        if data is None or len(data) == 0:
            print("  no data returned")
            continue
        print(f"  {len(data)} rows")
        frames.append(data)

    combined = pd.concat(frames, ignore_index=True) if frames else None
    return combined, failures


def output_name(row):
    return f"{row['databundelcode'].strip()}_{date.today():%Y%m%d}.csv"


def upload_dataframe(df, key):
    """Write a DataFrame to S3 as CSV, ``;`` separated like the other lookup files."""
    import boto3  # only needed in S3 mode, so the local run works without it

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, sep=";")
    boto3.client("s3").put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue().encode("utf-8"))
    print(f"Uploaded {len(df)} rows to s3://{BUCKET_NAME}/{key}")


def run(save):
    """Download every DT row and hand each result to ``save(dataframe, filename)``."""
    location_codes = load_location_codes()
    rows = load_rows_to_download()
    print(f"{len(rows)} row(s) with process = {PROCESS_DOWNLOAD}")

    results = []
    for _, row in rows.iterrows():
        databundelcode = row["databundelcode"].strip()
        entry = {"databundelcode": databundelcode, "file": None, "rows": 0, "failed": [], "error": None}
        try:
            combined, failures = download_row(row, location_codes)
            entry["failed"] = failures
            if combined is None:
                print(f"{databundelcode}: no data downloaded")
            else:
                entry["file"] = output_name(row)
                entry["rows"] = len(combined)
                save(combined, entry["file"])
        except Exception as exc:  # one bad row must not stop the others
            print(f"{databundelcode}: {exc}")
            entry["error"] = str(exc)
        results.append(entry)

    return {"downloaded": sum(1 for r in results if r["file"]), "results": results}


def lambda_handler(event, context):
    result = run(lambda df, filename: upload_dataframe(df, f"{S3_PREFIX}/{filename}"))
    result.update({"bucket": BUCKET_NAME, "prefix": S3_PREFIX})
    print(json.dumps(result))
    return result


if __name__ == "__main__":
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    os.makedirs(out_dir, exist_ok=True)

    def save_local(df, filename):
        path = os.path.join(out_dir, filename)
        df.to_csv(path, index=False, sep=";")
        print(f"Wrote {len(df)} rows to {path}")

    print(json.dumps(run(save_local), indent=2))
