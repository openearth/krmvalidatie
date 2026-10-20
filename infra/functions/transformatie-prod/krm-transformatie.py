import io
import json
import os
import zipfile
import boto3
import pandas as pd

BUCKET_NAME = os.environ.get("KRM_BUCKET_NAME", "krm-validatie-data-dev")
OUTPUT_PREFIX = os.environ.get("KRM_OUTPUT_PREFIX", "transformed")

s3 = boto3.client("s3")

# Waterinfo location code -> KRM location
LOCATIONS = {
    "bergenaanzee.standafvalmeetnet": "strand_Bergen",
    "noordwijk.strandafvalmeetnet": "strand_Noordwijk",
    "terschelling.strandafvalmeetnet": "strand_Terschelling",
    "veere.strandafvalmeetnet": "strand_Veere",
}

# KRM location -> its fixed point (X, Y) from KRM2_P.shp
POINTS = {
    "strand_Bergen": ("4.624250002", "52.64272501"),
    "strand_Noordwijk": ("4.489400001", "52.32181401"),
    "strand_Terschelling": ("5.393530006", "53.426058"),
    "strand_Veere": ("3.55688", "51.588564"),
}

# Bundle column <- Waterinfo column
COPY = {
    "MonsterCompartiment.code": "Compartiment.Code",
    "Grootheid.code": "Grootheid.Code",
    "Parameter.code": "Parameter.Code",
    "Parameter.Omschrijving": "Parameter.Omschrijving",
    "Eenheid.code": "Eenheid.Code",
    "Numeriekewaarde": "Meetwaarde.Waarde_Numeriek",
    "Kwaliteitsoordeel.code": "WaarnemingMetadata.Kwaliteitswaardecode",
    "Referentievlak.code": "WaarnemingMetadata.Referentievlak",
}

# Bundle column <- Waterinfo column or "NVT"
COPY_OR_NVT = {
    "Hoedanigheid.code": "Hoedanigheid.Code",
    "Orgaan.code": "Orgaan.Code",
    "Bemonsteringsapparaat.omschrijving": "BemonsteringsApparaat.Omschrijving",
    "Waardebewerkingsmethode.code": "WaardeBewerkingsMethode.Code",
}

# Bundle column = the same value in every row
FIXED = {
    "Meetobject.Namespace": "NL80",
    "Namespace": "NL80",
    "Referentiehorizontaal.code": "EPSG:4258",
    "BeginDiepte_m": "0",
    "EindDiepte_m": "0",
    "Waardebepalingsmethode.codespace": "www.rws.nl",
}

# All 83 columns
COLUMNS = (
    "Meetobject.Namespace;Meetobject.LokaalID;Namespace;Monster.lokaalID;"
    "Meetwaarde.lokaalID;MonsterCompartiment.code;Begindatum;Begintijd;Einddatum;"
    "Eindtijd;Tijd_UTCoffset;Typering.code;Grootheid.code;Parameter.code;"
    "Parameter.Omschrijving;Biotaxon.naam;Eenheid.code;Hoedanigheid.code;Limietsymbool;"
    "Numeriekewaarde;Alfanumeriekewaarde;Kwaliteitsoordeel.code;Orgaan.code;"
    "Organisme.naam;Bemonsteringsapparaat.omschrijving;Monsterophaaldatum;"
    "Monsterophaaltijd;GeometriePunt.X;GeometriePunt.Y;Referentiehorizontaal.code;"
    "BeginDiepte_m;EindDiepte_m;Referentievlak.code;Monsterbewerkingsmethode.code;"
    "Monsterbewerkingsmethode.codespace;Bemonsteringsmethode.code;"
    "Bemonsteringsmethode.codespace;Waardebepalingstechniek.code;ResultaatDatum;"
    "Analysecompartiment.code;Levensstadium.code;Lengteklasse.code;Geslacht.code;"
    "Verschijningsvorm.code;Levensvorm.code;Gedrag.code;Waardebewerkingsmethode.code;"
    "Waardebepalingsmethode.code;Waardebepalingsmethode.codespace;GeaccrediteerdJN;"
    "AccreditatieOrganisatie.code;Certificering.code;GerelateerdObservatieobject;"
    "GerelateerdObservatieobjectRol.id;GeoObject.lokaalID;"
    "OpdrachtreferentieOpdrachtgever;OpdrachtreferentieUitvoerder;Opdracht.Namespace;"
    "Opdracht.lokaalID;Onderzoekssoort.id;Monstertype.id;Bemonsteringssoort.code;"
    "Monstercriterium.code;Bemonsteringsapparaat.codespace;GerelateerdMonsterobject;"
    "GerelateerdMonsterobjectRol.id;Monster.Opmerking;Resultaattijd;Meetapparaat.code;"
    "Meetapparaat.codespace;Meetinstantie.code;Meetinstantie.codespace;"
    "LocatieTypeWaardeBepaling.id;Volgnummer;MeetwaardeOpmerking;Recovery;Spread;"
    "MeasurementUncertainty;Z-score;Drift;AutomatedControl.code;ExpertControl.code;"
    "ValidationScore.code"
).split(";")

# -----------------------------------------------------------------------------


def lambda_handler(event, context):
    if "Records" in event:  # came through SNS: unwrap the message
        event = json.loads(event["Records"][0]["Sns"]["Message"])
    bundle = event["bundle"]
    input_key = event["input"]

    # 1. Read the CSV of the downloader (';' separated)
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=input_key)
    data = pd.read_csv(io.BytesIO(obj["Body"].read()), sep=";", dtype=str)
    rows_in = len(data)

    # 2. Sort by location and time
    data["krm"] = data["Code"].map(LOCATIONS)
    data["time"] = pd.to_datetime(data["Tijdstip"])
    data = data.sort_values(["krm", "time"])

    # 3. Fill the bundle columns
    out = pd.DataFrame(index=data.index)
    for column, value in FIXED.items():
        out[column] = value
    for column, source in COPY.items():
        out[column] = data[source]
    for column, source in COPY_OR_NVT.items():
        out[column] = data[source].fillna("NVT")

    out["Numeriekewaarde"] = out["Numeriekewaarde"].str.replace(r"\.0$", "", regex=True)  # "16.0" -> "16"
    out["Kwaliteitsoordeel.code"] = out["Kwaliteitsoordeel.code"].str.zfill(2)  # "0" -> "00"
    method = data["WaardeBepalingsMethode.Code"].str.replace("other:", "", regex=False)
    out["Waardebepalingsmethode.code"] = method + "_" + data["WaardeBepalingsMethode.Omschrijving"]

    day = (  # D-M-YYYY
        data["time"].dt.day.astype(str)
        + "-" + data["time"].dt.month.astype(str)
        + "-" + data["time"].dt.year.astype(str)
    )
    out["Begindatum"] = day
    out["ResultaatDatum"] = day
    out["Begintijd"] = data["time"].dt.strftime("%H:%M")

    out["Meetobject.LokaalID"] = "NL80_" + data["krm"]
    out["GeometriePunt.X"] = data["krm"].map(lambda krm: POINTS[krm][0])
    out["GeometriePunt.Y"] = data["krm"].map(lambda krm: POINTS[krm][1])

    # 4. IDs like: NL80_RWS2025_strand_Bergen_OR_2025-01-14,

    iso_day = data["time"].dt.strftime("%Y-%m-%d")
    data_year = data["time"].dt.year.astype(str)
    out["Monster.lokaalID"] = (
        "NL80_RWS" + data_year + "_" + data["krm"] + "_" + out["MonsterCompartiment.code"] + "_" + iso_day
    )
    row_number = pd.Series(range(1, len(out) + 1), index=out.index).astype(str)
    out["Meetwaarde.lokaalID"] = out["Monster.lokaalID"] + "_" + row_number

    # 5. Write the ZIP: one CSV with the 83 columns, ';' and cp1252

    output_key = f"{OUTPUT_PREFIX}/{bundle}.zip"
    csv_text = out.reindex(columns=COLUMNS).to_csv(sep=";", index=False)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{bundle}.csv", csv_text.encode("cp1252", errors="replace"))
    s3.put_object(Bucket=BUCKET_NAME, Key=output_key, Body=buffer.getvalue())

    result = {
        "bucket": BUCKET_NAME,
        "bundle": bundle,
        "input": input_key,
        "rows_in": rows_in,
        "rows_out": len(out),
        "output": output_key,
    }
    print(json.dumps(result))
    return result
