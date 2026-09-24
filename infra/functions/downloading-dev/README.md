# Waterinfo downloader v2 (dev)

Deliberately simple version: one flat script, one settings file. Nothing is hardcoded in the
script — every value comes from `config.toml`.

| File | Purpose |
| --- | --- |
| `download.py` | The script, close to the original demo |
| `config.toml` | All settings |
| `Dockerfile` | Standalone image (`runtime` and `notebook` stages) |
| `docker-compose.yml` | Standalone compose stack |
| `requirements.txt` | `pandas`, `rws-waterinfo` |
| `requirements-notebook.txt` | Extra dependencies for JupyterLab |
| `notebooks/waterinfo_demo.ipynb` | Interactive version of the demo script |

TOML was chosen over JSON because it allows comments and Python 3.11 reads it with the
built-in `tomllib`, so no extra dependency is needed.

## Running it

```bash
cd infra/functions/downloading-dev

# Build the images once
docker compose build

# Run the download
docker compose run --rm downloader
```

The repository root is mounted at `/work`, so the relative `output_dir` in `config.toml`
resolves to the repository `data/` folder exactly as it would when running locally.

Point the script at a different settings file by passing it as an argument:

```bash
docker compose run --rm downloader my-other-config.toml
```

## Experimenting in JupyterLab

```bash
docker compose up -d notebook
```

Then open: **http://127.0.0.1:8888/lab?token=krm**

`notebooks/waterinfo_demo.ipynb` contains the demo split into steps: fetch the catalog, build
the location/parameter subsets, do a single request, loop over all locations, write the result
to `data/waterinfo/notebook/`, and read back `config.toml` to compare with the script.

Notebooks are mounted read-write, so anything you save lands back in the repository. Change
the token with `JUPYTER_TOKEN=... docker compose up -d notebook`. The port is bound to
`127.0.0.1` only. Stop everything with:

```bash
docker compose down
```

## Settings

All of these live in `config.toml`:

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
| `output_dir` | `../../../data/waterinfo_v2` | Output folder, relative to `config.toml` |

## Output

One CSV per location plus a combined CSV in `data/waterinfo_v2/` (gitignored):

```
data/waterinfo_v2/
├── AANTPLTE_combined.csv
├── bergenaanzee.standafvalmeetnet.csv
├── noordwijk.strandafvalmeetnet.csv
├── terschelling.strandafvalmeetnet.csv
└── veere.strandafvalmeetnet.csv
```

## Difference with the original demo

The demo looped over locations **and** parameters, but the request only used the location and
the fixed grootheid code — `param` was never used, so the same data was fetched 171 times per
location. This version loops over locations only.
