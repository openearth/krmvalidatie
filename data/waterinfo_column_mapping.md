# Waterinfo column mapping

Column names and their descriptions for the 83 output columns. Empty descriptions mean the field stays blank.

Source notation: `(WI)` means the value comes from the Waterinfo table.

| Column name | Description |
| --- | --- |
| Meetobject.Namespace | Constant value: `NL80`. |
| Meetobject.LokaalID | Construct by concatenating `NL80` + `Code` (WI), formatted according to the required identifier format. |
| Namespace | Constant value: `NL80`. |
| Monster.LokaalID | Construct by concatenating `NL80_RWS` + `[YEAR]` + `Code` (WI) + `Compartiment.Code` (WI) + `Tijdstip` (WI), with each component formatted according to the required identifier format. |
| Meetwaarde.LokaalID | Construct by concatenating `Meetobject.LokaalID` + a unique sequential number for each row. |
| MonsterCompartiment.code | Value from `Compartiment.Code` (WI). |
| Begindatum | Value from `Tijdstip` (WI), formatted as `D-M-YYYY`. |
| Begintijd | Value from `Tijdstip` (WI), formatted as `HH`. |
| Einddatum | |
| Eindtijd | |
| Tijd_UTCoffset | |
| Typering.code | |
| Grootheid.code | Value from `Grootheid.Code` (WI). |
| Parameter.code | Value from `Parameter.Code` (WI). |
| Parameter.Omschrijving | Value from `Parameter.Omschrijving` (WI). |
| Biotaxon.naam | |
| Eenheid.code | |
| Hoedanigheid.code | Value from `Hoedanigheid.code`. If empty, replace with `NVT`. |
| Limietsymbool | |
| Numeriekewaarde | Value from `Meetwaarde.Waarde_Numeriek` (WI). |
| Alfanumeriekewaarde | |
| Kwaliteitsoordeel.code | Value from `WaarnemingMetadata.Kwaliteitswaardecode` (WI). Treat as a string and preserve two-digit formatting: `0` → `"00"`, `1` → `"01"`, and so on. |
| Orgaan.code | Value from `Orgaan.code` (WI). If empty, replace with `NVT`. |
| Organisme.naam | |
| Bemonsteringsapparaat.omschrijving | Value from `Bemonsteringsapparaat.omschrijving` (WI). If empty, replace with `NVT`. |
| Monsterophaaldatum | |
| Monsterophaaltijd | |
| GeometriePunt.X | Value from longitude (`Lon`) (WI). |
| GeometriePunt.Y | Value from latitude (`Lat`) (WI). |
| Referentiehorizontaal.code | Constant value: `EPSG:4258`, equivalent to ETRS89 (European Terrestrial Reference System 1989). |
| BeginDiepte_m | Constant value: `0`. |
| EindDiepte_m | Constant value: `0`. |
| Referentievlak.code | Value from `WaarnemingMetadata.Referentievlak` (WI). |
| Monsterbewerkingsmethode.code | |
| Monsterbewerkingsmethode.codespace | |
| Bemonsteringsmethode.code | |
| Bemonsteringsmethode.codespace | |
| Waardebepalingstechniek.code | |
| ResultaatDatum | Value from `Tijdstip` (WI), formatted as `D-M-YYYY`. |
| Analysecompartiment.code | |
| Levensstadium.code | |
| Lengteklasse.code | |
| Geslacht.code | |
| Verschijningsvorm_code | |
| Levensvorm.code | |
| Gedrag.code | |
| Waardebewerkingsmethode.code | Value from `Waardebewerkingsmethode.code`. If empty, replace with `NVT`. |
| Waardebepalingsmethode.code | Construct by combining `WaardeBepalingsMethode.Code` (WI) + `WaardeBepalingsMethode.Omschrijving` (WI), formatted according to the required identifier format. |
| Waardebepalingsmethode_codespace | Constant value: `www.rws.nl` for Waterinfo (WI) data. |
| GeaccrediteerdJN | |
| AccreditatieOrganisatie.code | |
| Certificering.code | |
| GerelateerdObservatieobject | |
| GerelateerdObservatieobjectRol.id | |
| GeoObject.lokaalID | |
| OpdrachtreferentieOpdrachtgever | |
| OpdrachtreferentieUitvoerder | |
| Opdracht.Namespace | |
| Opdracht.lokaalID | |
| Onderzoekssoort.id | |
| Monstertype.id | |
| Bemonsteringssoort.code | |
| Monstercriterium.code | |
| Bemonsteringsapparaat.codespace | |
| GerelateerdMonsterobject | |
| GerelateerdMonsterobjectRol.id | |
| Monster.Opmerking | |
| Resultaattijd | |
| Meetapparaat.code | |
| Meetapparaat.codespace | |
| Meetinstantie.code | |
| Meetinstantie.codespace | |
| LocatieTypeWaardeBepaling.id | |
| Volgnummer | |
| MeetwaardeOpmerking | |
| Recovery | |
| Spread | |
| MeasurementUncertainty | |
| Z-score | |
| Drift | |
| AutomatedControl.code | |
| ExpertControl.code | |
| ValidationScore.code | |

## Formatting note

For `Kwaliteitsoordeel.code`, store the value as a string, not a number, so leading zeros are preserved (e.g. `"00"`, `"01"`, `"02"`).
