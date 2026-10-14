# Waterinfo downloader (dev)

Downloads Rijkswaterstaat Waterinfo observation data with the
[`rws-waterinfo`](https://pypi.org/project/rws-waterinfo/) package.

> 👉 If you only need to **start a download from the AWS console** and **change the settings on
> GitHub**, read [`USER_GUIDE.md`](USER_GUIDE.md) instead. This README is the technical
> documentation.

It runs in two places from one set of settings:

- **locally**, through Docker Compose, writing CSV files to the repository `data/` folder
- **on AWS**, as the `krm-downloading-lambda-<workspace>` Lambda, writing CSV files to the
  `downloaded/` prefix of the `krm-validatie-data-<workspace>` bucket

## Settings

All settings live in **`data/waterinfo_downloading_settings.toml`** in the repository root
`data/` folder, next to `validatielijst.csv` and the other lookup tables. Nothing is hardcoded
in the scripts.

The Lambda reads that file over GitHub raw at runtime, exactly like the validation Lambda reads
its lookup tables. So changing what gets downloaded means editing and committing the TOML — no
redeploy needed.

TOML was chosen over JSON because it allows comments and Python 3.11 reads it with the built-in
`tomllib`, so no extra dependency is needed.

| Key | Default | Description |
| --- | --- | --- |
| `method_code` | `other:X156` | Catalog filter on `WaardeBepalingsMethode.Code` |
| `compartiment_code` | `OR` | Compartiment code |
| `eenheid_code` | *(empty)* | Eenheid code |
| `meetapparaat_code` | *(empty)* | Meetapparaat code |
| `grootheid_code` | `AANTPLTE` | Grootheid code to request |
| `start_date` | `2022-12-31` | Start of the period |
| `end_date` | `2023-12-31` | End of the period |
| `max_workers` | `8` | Parallel downloads per request |
| `limit_locations` | `0` | Only the first N locations, 0 = all |
| `output_dir` | `waterinfo` | Local script only, relative to the TOML file |
| `s3_prefix` | `downloaded` | Lambda only, prefix inside the bucket |

## Running locally

```bash
cd infra/functions/downloading-dev

docker compose build          # once
docker compose run --rm downloader
```

The repository root is mounted at `/work`, so the relative `output_dir` resolves to
`data/waterinfo/` on the host. Use a different settings file with:

```bash
docker compose run --rm downloader /work/data/my-other-settings.toml
```

## Experimenting in JupyterLab

```bash
docker compose up -d notebook
```

Then open **http://127.0.0.1:8888/lab?token=krm**. `notebooks/waterinfo_demo.ipynb` walks
through the steps interactively. Notebooks are mounted read-write, so saves land back in the
repository. Stop everything with `docker compose down`.

## Running on AWS

`krm-downloading.py` is the Lambda handler. It is triggered in two ways.

### 1. Manually, by publishing a message

```bash
aws sns publish --region eu-west-1 \
  --topic-arn arn:aws:sns:eu-west-1:637423531264:DownloadWaterinfo-dev \
  --message '{}'
```

An **empty message `{}` means the TOML is used unchanged** — this is the normal case.

If you want to deviate for a single run without editing the TOML, put the settings you want to
change in the message as JSON. Anything you do not mention keeps its TOML value:

```bash
aws sns publish --region eu-west-1 \
  --topic-arn arn:aws:sns:eu-west-1:637423531264:DownloadWaterinfo-dev \
  --message '{"grootheid_code": "WATHTE", "compartiment_code": "OW", "start_date": "2024-01-01", "end_date": "2024-02-01"}'
```

Overridable keys: `method_code`, `compartiment_code`, `eenheid_code`, `meetapparaat_code`,
`grootheid_code`, `start_date`, `end_date`, `max_workers`, `limit_locations`, `s3_prefix`.
Unknown keys are ignored and logged. A message that is not JSON is ignored, and the TOML is
used unchanged.

> **On Windows PowerShell**, inline JSON gets mangled by the shell's quoting, and the message
> silently arrives as plain text (the Lambda then falls back to the TOML). Put the message in a
> file instead:
>
> ```powershell
> '{"limit_locations": 1}' | Out-File msg.json -Encoding ascii -NoNewline
> aws sns publish --region eu-west-1 `
>   --topic-arn arn:aws:sns:eu-west-1:637423531264:DownloadWaterinfo-dev `
>   --message file://msg.json
> ```

### 2. On a schedule

An EventBridge rule (`DownloadWaterinfoSchedule-<workspace>`) invokes the Lambda with an empty
payload, so it uses the TOML unchanged. It is **disabled by default**; enable it by setting
`waterinfo_schedule_enabled = true` and adjust `waterinfo_schedule_expression`
(default `cron(0 3 * * ? *)`, nightly at 03:00 UTC) in `infra/variables.tf`.

### Output in S3

```
s3://krm-validatie-data-dev/downloaded/<grootheid_code>/
├── <location>.csv
└── <grootheid_code>_combined.csv
```

Each run **deletes everything under its own prefix before downloading**, so the folder always
reflects exactly one run. Without this, lowering `limit_locations` or changing the location set
would leave stale CSV files from the previous run next to the fresh ones, indistinguishable from
them. The deletion happens after the catalog has been fetched successfully, so a failure to
reach Waterinfo leaves the existing data untouched. The number of deleted files is reported as
`removed` in the result.

The local script does the same with `*.csv` in its `output_dir`.

## Lambda layer

The Lambda uses two layers: the existing `geopandas` layer for pandas, and a small
`rws-waterinfo` layer (~2 MB) for the Waterinfo client and `requests`. To rebuild and publish a
new version:

```bash
cd infra/layers
mkdir -p rws-waterinfo-layer
docker run --rm --entrypoint /bin/sh -v "$PWD/rws-waterinfo-layer:/asset" public.ecr.aws/lambda/python:3.11 \
  -c "pip install --no-cache-dir --no-deps --target /asset/python rws-waterinfo && pip install --no-cache-dir --target /asset/python requests"
cd rws-waterinfo-layer && zip -r ../rws-waterinfo-layer.zip python && cd ..
aws lambda publish-layer-version --region eu-west-1 --layer-name rws-waterinfo \
  --zip-file fileb://rws-waterinfo-layer.zip --compatible-runtimes python3.11 \
  --compatible-architectures x86_64
```

`rws-waterinfo` is installed with `--no-deps` on purpose: its pandas dependency would otherwise
try to build numpy from source, and pandas already comes from the geopandas layer. Afterwards
update the layer version in `infra/lambda.tf`.

## Files

| File | Purpose |
| --- | --- |
| `download.py` | Local script |
| `krm-downloading.py` | Lambda handler |
| `Dockerfile` | Local image, `runtime` and `notebook` stages |
| `docker-compose.yml` | Local compose stack |
| `requirements.txt` | `pandas`, `rws-waterinfo` |
| `requirements-notebook.txt` | JupyterLab extras, kept out of the runtime image |
| `notebooks/waterinfo_demo.ipynb` | Interactive version of the demo |
| `USER_GUIDE.md` | Step-by-step manual for console/GitHub users |

The handler is duplicated to `../downloading-prod/` because Terraform reads the function source
from `functions/downloading-${terraform.workspace}`, the same pattern as `validatie-*` and
`publicatie-*`.

## Difference with the original demo script

The demo looped over locations **and** parameters, but the request only used the location and
the fixed grootheid code — `param` was never used, so the same data was fetched 171 times per
location. Both the script and the Lambda loop over locations only.
