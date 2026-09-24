"""Lambda handler that downloads Waterinfo data and stores it in S3.

Triggered by a message on the DownloadWaterinfo SNS topic, or by the scheduled
EventBridge rule.

Settings come from ``data/waterinfo_downloading_settings.toml`` in the repository,
read over GitHub raw, the same way the other lookup tables are loaded. An SNS
message may carry a JSON object to override individual settings for a single run;
an empty message means the TOML is used unchanged.
"""

import io
import json
import os
import tomllib
import urllib.request

import boto3
import pandas as pd
import rws_waterinfo as rw

SETTINGS_URL = os.environ.get(
    "KRM_SETTINGS_URL",
    "https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data/waterinfo_downloading_settings.toml",
)
BUCKET_NAME = os.environ.get("KRM_BUCKET_NAME", "krm-validatie-data-dev")
DEFAULT_PREFIX = os.environ.get("KRM_S3_PREFIX", "downloaded")

# Settings a message is allowed to override. Anything else in the message is ignored.
OVERRIDABLE = {
    "method_code",
    "compartiment_code",
    "eenheid_code",
    "meetapparaat_code",
    "grootheid_code",
    "start_date",
    "end_date",
    "max_workers",
    "limit_locations",
    "s3_prefix",
}

s3 = boto3.client("s3")


def load_settings():
    """Read the TOML settings file from GitHub raw."""
    print(f"Reading settings from {SETTINGS_URL}")
    with urllib.request.urlopen(SETTINGS_URL, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"Could not read settings: HTTP {response.status}")
        return tomllib.loads(response.read().decode("utf-8"))


def extract_overrides(event):
    """Pull optional setting overrides out of an SNS message.

    Returns an empty dict for a scheduled run, an empty message, or a message
    that is not a JSON object, so the TOML is then used unchanged.
    """
    if not isinstance(event, dict):
        return {}

    messages = []
    for record in event.get("Records", []):
        if record.get("EventSource") == "aws:sns":
            messages.append(record["Sns"].get("Message", ""))

    # Allow a direct test invoke with the payload as the event itself
    if not messages and not event.get("Records"):
        messages.append(json.dumps(event))

    overrides = {}
    for message in messages:
        if not message or not message.strip():
            continue
        try:
            parsed = json.loads(message)
        except json.JSONDecodeError:
            print(f"Message is not JSON, ignoring it and using the TOML: {message!r}")
            continue
        if not isinstance(parsed, dict):
            continue
        for key, value in parsed.items():
            if key in OVERRIDABLE:
                overrides[key] = value
            else:
                print(f"Ignoring unknown setting in message: {key}")
    return overrides


def upload_dataframe(df, key):
    """Write a DataFrame to S3 as CSV."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue().encode("utf-8"))
    print(f"Uploaded {len(df)} rows to s3://{BUCKET_NAME}/{key}")


def lambda_handler(event, context):
    cfg = load_settings()
    overrides = extract_overrides(event)
    if overrides:
        print(f"Applying overrides from message: {overrides}")
        cfg.update(overrides)

    prefix = f"{cfg.get('s3_prefix', DEFAULT_PREFIX)}/{cfg['grootheid_code']}"

    catalog = rw.get_catalog()
    subset = catalog[catalog["WaardeBepalingsMethode.Code"] == cfg["method_code"]]
    loccatalog = subset.groupby("Code").first().reset_index()
    print(f"{len(subset)} catalog records, {len(loccatalog)} unique locations")

    locations = loccatalog["Code"]
    if cfg.get("limit_locations"):
        locations = locations.head(cfg["limit_locations"])

    frames = []
    failures = []
    for loc in locations:
        params = [
            [
                cfg["compartiment_code"],
                cfg["eenheid_code"],
                cfg["meetapparaat_code"],
                cfg["grootheid_code"],
                f"{loc}",
                cfg["start_date"],
                cfg["end_date"],
            ]
        ]
        print(f"Processing location: {loc}")
        try:
            data = rw.get_data(params=params, return_df=True, max_workers=cfg["max_workers"])
        except Exception as exc:  # one bad location must not stop the run
            print(f"  failed: {exc}")
            failures.append({"location": loc, "error": str(exc)})
            continue

        if data is None or len(data) == 0:
            print("  no data returned")
            continue

        data["krm_location_code"] = loc
        upload_dataframe(data, f"{prefix}/{loc}.csv")
        frames.append(data)

    result = {
        "bucket": BUCKET_NAME,
        "prefix": prefix,
        "locations": len(locations),
        "downloaded": len(frames),
        "failed": failures,
        "rows": 0,
    }

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        upload_dataframe(combined, f"{prefix}/{cfg['grootheid_code']}_combined.csv")
        result["rows"] = len(combined)
    else:
        print("no data downloaded")

    print(json.dumps(result))
    return result
