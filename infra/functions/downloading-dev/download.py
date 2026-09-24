# project information on https://pypi.org/project/rws-waterinfo/
# Simple version: all settings come from config.toml, nothing is hardcoded here.

import sys
import tomllib
from pathlib import Path

import pandas as pd
import rws_waterinfo as rw

# read the settings; by default the shared file in the repository data folder,
# the same place where the other lookup tables live
DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "data" / "waterinfo_downloading_settings.toml"

config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CONFIG
with open(config_path, "rb") as f:
    cfg = tomllib.load(f)

# relative output paths are resolved against the config file
output_dir = Path(cfg["output_dir"])
if not output_dir.is_absolute():
    output_dir = (config_path.parent / output_dir).resolve()
output_dir.mkdir(parents=True, exist_ok=True)

catalog = rw.get_catalog()
# check the initial catalog content
# print(catalog.head())

# create unique list of locations based on the 'WaardeBepalingsMethode.Code' column
# that matches the configured method code
loccatalog = (
    catalog[catalog["WaardeBepalingsMethode.Code"] == cfg["method_code"]]
    .groupby("Code")
    .first()
    .reset_index()
)

# create unique list of parameters based on the 'WaardeBepalingsMethode.Code' column
# that matches the configured method code
paramcatalog = (
    catalog[catalog["WaardeBepalingsMethode.Code"] == cfg["method_code"]]
    .groupby("Parameter_Wat_Omschrijving")
    .first()
    .reset_index()
)

print(f"locations: {len(loccatalog)}, parameters: {len(paramcatalog)}")

locations = loccatalog["Code"]
if cfg["limit_locations"]:
    locations = locations.head(cfg["limit_locations"])

frames = []
for loc in locations:
    # use loc to fill in the request parameters
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
    data = rw.get_data(params=params, return_df=True, max_workers=cfg["max_workers"])

    if data is None or len(data) == 0:
        print("  no data returned")
        continue

    data["krm_location_code"] = loc
    data.to_csv(output_dir / f"{loc}.csv", index=False)
    frames.append(data)
    print(f"  {len(data)} rows")

if frames:
    combined = pd.concat(frames, ignore_index=True)
    combined_path = output_dir / f"{cfg['grootheid_code']}_combined.csv"
    combined.to_csv(combined_path, index=False)
    print(f"wrote {len(combined)} rows to {combined_path}")
else:
    print("no data downloaded")
