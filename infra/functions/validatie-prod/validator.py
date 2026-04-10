"""Main KRM validation logic."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

from .report import ValidationReport, ValidationSection

if TYPE_CHECKING:
    from config import ValidationConfig
    from reference_data import ReferenceDataLoader


class KRMValidator:
    """
    Main validator class for KRM data bundles.
    
    Performs all validation checks and collects results into a ValidationReport.
    """
    
    # Valid values for fixed-value checks
    ALLOWED_KWALITEITSOORDEEL = {'00', '03', '04', '25', '99', 0, 3, 4, 25, 99}
    ALLOWED_REFERENTIEHORIZONTAAL = {'EPSG:4258', 'EPSG4258'}
    
    def __init__(self, config: "ValidationConfig", ref_data: "ReferenceDataLoader"):
        self.config = config
        self.ref_data = ref_data
        self.report = ValidationReport()
    
    def validate(self, gdf: gpd.GeoDataFrame, package_name: str) -> ValidationReport:
        """
        Run all validation checks on the data bundle.
        
        Args:
            gdf: GeoDataFrame containing the data to validate
            package_name: Name of the data bundle
            
        Returns:
            ValidationReport containing all failures found
        """
        clean_name = package_name.replace('+', ' ')
        
        # Determine validation rules for each record
        rules = self._determine_rules(gdf, clean_name)
        
        # Run all validation checks
        self._check_geo_control(gdf, clean_name)
        self._check_mandatory_columns(gdf, clean_name)
        self._check_column_values(gdf, clean_name)
        self._check_counts(gdf, clean_name, rules)
        self._check_parameters(gdf, clean_name, rules)
        self._check_parameter_aggregates(gdf, clean_name, rules)
        self._check_fixed_values(gdf, clean_name)
        self._check_rules(rules)
        self._check_other(gdf, clean_name)
        self._check_date_range(gdf, clean_name)
        
        return self.report
    
    # -------------------------------------------------------------------------
    # Rule Determination
    # -------------------------------------------------------------------------
    
    def _determine_rules(self, gdf: gpd.GeoDataFrame, package_name: str) -> pd.DataFrame:
        """Determine which validation rule applies to each record."""
        validatieregels = self.ref_data.get_validation_rules_exploded(package_name)
        group = self.ref_data.get_groups_for_rules(package_name)
        
        if validatieregels.empty:
            return pd.DataFrame(columns=[
                'databundelcode', 'record_id', 'uitvalreden',
                'mogelijke_validatieregels', 'validatieregel',
                'betreftverzameling', 'monster_identificatie'
            ])
        
        # Prepare data
        df = gdf.copy()
        df['parameter'] = df.apply(self._get_parameter_value, axis=1)
        df['record_id'] = df['meetwaarde.lokaalid'].str.replace('NL80_', '')
        df['locatiecode'] = df['meetobject.lokaalid'].str.replace('NL80_', '')
        
        results = []
        for _, row in df.iterrows():
            matched_rules = self._find_matching_rules(row, validatieregels, group)
            found_group = group[group['parameter'].str.lower() == row['parameter']]
            
            results.append({
                'databundelcode': package_name,
                'record_id': row['record_id'],
                'uitvalreden': 5 if not matched_rules else 0,
                'mogelijke_validatieregels': list(set(matched_rules)),
                'validatieregel': matched_rules[0] if matched_rules else None,
                'betreftverzameling': 1 if len(found_group) > 1 else 0,
                'monster_identificatie': row['monster.lokaalid']
            })
        
        return pd.DataFrame(results)
    
    def _find_matching_rules(
        self,
        row: pd.Series,
        validatieregels: pd.DataFrame,
        group: pd.DataFrame
    ) -> list[int]:
        """Find all validation rules that match a data row."""
        matched = []
        found_group = group[group['parameter'].str.lower() == row['parameter']]
        begindatum = pd.to_datetime(row.get('begindatum'), errors='coerce', format='mixed')
        
        for idx, rule in validatieregels.iterrows():
            if self._rule_matches(row, rule, found_group, begindatum):
                matched.append(idx)
        
        return matched
    
    def _rule_matches(
        self,
        row: pd.Series,
        rule: pd.Series,
        found_group: pd.DataFrame,
        begindatum: pd.Timestamp
    ) -> bool:
        """Check if a validation rule matches a data row."""
        # Column match checks
        match_columns = [
            ('eenheid.code', 'eenheid_code'),
            ('grootheid.code', 'grootheid_code'),
            ('typering.code', 'typering_code'),
            ('hoedanigheid.code', 'hoedanigheid_code'),
            ('monstercompartiment.code', 'monstercompartiment_code'),
            ('waardebewerkingsmethode.code', 'waardebewerkingsmethode_code'),
            ('orgaan.code', 'orgaan_code'),
            ('organisme.naam', 'organisme_naam'),
        ]
        
        for data_col, rule_col in match_columns:
            if not self._values_match(row.get(data_col), rule.get(rule_col)):
                return False
        
        # Semicolon-separated value checks
        if not self._value_in_list(row.get('locatiecode'), rule.get('locatiecode', '')):
            return False
        if not self._value_in_list(
            row.get('bemonsteringsapparaat.omschrijving'),
            rule.get('bemonsteringsapparaat_omschrijving', '')
        ):
            return False
        
        # Group check
        if not (len(found_group) >= 1 or pd.isna(row['parameter'])):
            return False
        
        # Date range check
        if not (
            pd.notna(begindatum) and
            pd.notna(rule['startdatum']) and
            pd.notna(rule['einddatum']) and
            rule['startdatum'] <= begindatum <= rule['einddatum']
        ):
            return False
        
        # Biotaxon check
        if pd.notna(row['biotaxon.naam']) and rule.get('biotaxon_of_niet', '').lower() != 'j':
            return False
        
        return True
    
    # -------------------------------------------------------------------------
    # Validation Checks
    # -------------------------------------------------------------------------
    
    def _check_geo_control(self, gdf: gpd.GeoDataFrame, package_name: str) -> None:
        """Check geographic validity of locations."""
        location_gdf = self.ref_data.location_gdf
        valid_locations = self.ref_data.location_identifiers
        
        # Prepare data
        df = gdf.copy()
        df['locatie.code'] = df['meetobject.lokaalid'].str.replace('NL80_', '')
        
        # Create point geometries
        points = [Point(xy) for xy in zip(df['geometriepunt.x'], df['geometriepunt.y'])]
        gdf_array = gpd.GeoDataFrame(df, geometry=points, crs="EPSG:4258")
        gdf_array['cleaned_id'] = gdf_array['meetobject.lokaalid'].str.replace('NL80_', '')
        
        # Check for unknown locations
        unknown_mask = ~gdf_array['cleaned_id'].isin(valid_locations)
        for _, row in gdf_array[unknown_mask].iterrows():
            self.report.add(
                section=ValidationSection.GEO_CONTROL,
                databundelcode=package_name,
                record_id=row['meetwaarde.lokaalid'],
                uitvalreden='onbekende locatie',
                informatie=f"onbekende locatie: {row['locatie.code']}"
            )
        
        # Check distances for known locations
        self._check_location_distances(gdf_array, location_gdf, package_name)
    
    def _check_location_distances(
        self,
        gdf_array: gpd.GeoDataFrame,
        location_gdf: gpd.GeoDataFrame,
        package_name: str
    ) -> None:
        """Check if data points are within threshold distance of reference locations."""
        # Project to UTM for accurate distance calculation
        gdf_proj = location_gdf.to_crs("EPSG:32631")
        gdf_array_proj = gdf_array.to_crs("EPSG:32631")
        
        merged = gdf_array_proj.merge(
            gdf_proj[['MPNIDENT', 'geometry']],
            left_on='cleaned_id',
            right_on='MPNIDENT',
            suffixes=('_array', '_shapefile')
        )
        
        max_distance = self.config.max_location_distance_m
        
        for _, row in merged.iterrows():
            distance = row['geometry_array'].distance(row['geometry_shapefile'])
            if distance > max_distance:
                self.report.add(
                    section=ValidationSection.GEO_CONTROL,
                    databundelcode=package_name,
                    record_id=row['meetwaarde.lokaalid'],
                    uitvalreden='locatie verder dan 100 meter',
                    informatie=f"afstand van locatie: {row['locatie.code']}: {int(distance)}m"
                )
    
    def _check_mandatory_columns(self, gdf: gpd.GeoDataFrame, package_name: str) -> None:
        """Check that mandatory columns are not empty."""
        column_def = self.ref_data.column_definition
        mandatory_cols = column_def[column_def['ihm_verplicht'] == 'V']['kolomnaam'].str.lower()
        
        for col in mandatory_cols:
            if col not in gdf.columns:
                continue
            
            missing_mask = gdf[col].isna()
            for _, row in gdf[missing_mask].iterrows():
                self.report.add(
                    section=ValidationSection.COLUMN_CHECK,
                    databundelcode=package_name,
                    record_id=row['meetwaarde.lokaalid'],
                    uitvalreden='verplichte kolom is leeg',
                    informatie=f"geen waarde in bestand voor: {col}"
                )
    
    def _check_column_values(self, gdf: gpd.GeoDataFrame, package_name: str) -> None:
        """Check that column values match validation rules."""
        rules = self.ref_data.get_validation_rules(package_name)
        if rules.empty:
            return
        
        df = gdf.copy()
        df['locatie.code'] = df['meetobject.lokaalid'].str.replace('NL80_', '')
        
        # Columns to validate
        check_columns = [
            ('grootheid.code', 'grootheid_code', 'Grootheid.code'),
            ('typering.code', 'typering_code', 'Typering.code'),
            ('eenheid.code', 'eenheid_code', 'Eenheid.code'),
            ('hoedanigheid.code', 'hoedanigheid_code', 'Hoedanigheid.code'),
            ('waardebewerkingsmethode.code', 'waardebewerkingsmethode_code', 'Waardebewerkingsmethode'),
            ('monstercompartiment.code', 'monstercompartiment_code', 'Compartimentcode'),
            ('bemonsteringsapparaat.omschrijving', 'bemonsteringsapparaat_omschrijving', 'Veldapparaatomschrijving'),
            ('organisme.naam', 'organisme_naam', 'Organismenaam'),
        ]
        
        # Ensure rule columns are strings
        for _, rule_col, _ in check_columns:
            if rule_col in rules.columns:
                rules[rule_col] = rules[rule_col].astype(str)
        
        total_rules = len(rules)
        
        for _, row in df.iterrows():
            mismatch_counts = self._count_mismatches(row, rules, check_columns)
            
            # Find column that fails all rules
            for data_col, rule_col, display_name in check_columns:
                if mismatch_counts.get(data_col, 0) == total_rules:
                    valid_values = ','.join(rules[rule_col].unique())
                    self.report.add(
                        section=ValidationSection.COLUMN_VALUE,
                        databundelcode=package_name,
                        record_id=row['meetwaarde.lokaalid'],
                        uitvalreden='ongeldige code',
                        informatie=f"{display_name} '{row[data_col]}' niet in: {{{valid_values}}}"
                    )
                    break
            else:
                # Check location code separately
                loc_mismatches = sum(
                    1 for _, rule in rules.iterrows()
                    if not self._location_matches(row, rule)
                )
                if loc_mismatches == total_rules:
                    valid_locs = ','.join(rules['locatiecode'].astype(str).unique())
                    self.report.add(
                        section=ValidationSection.COLUMN_VALUE,
                        databundelcode=package_name,
                        record_id=row['meetwaarde.lokaalid'],
                        uitvalreden='ongeldige code',
                        informatie=f"Locatiecode '{row['locatie.code']}' niet in: {{{valid_locs}}}"
                    )
    
    def _check_counts(
        self,
        gdf: gpd.GeoDataFrame,
        package_name: str,
        rules: pd.DataFrame
    ) -> None:
        """
        Check record counts against expected values.
        
        Validates that the number of records (monsters or tijdwaarden) matches
        the expected count defined in validation rules.
        """
        validatie_regels = self.ref_data.get_validation_rules_exploded(package_name)
        group = self.ref_data.get_groups_for_rules(package_name)
        
        if validatie_regels.empty or rules.empty:
            return
        
        # Prepare data
        df = gdf.copy()
        df['cleaned_lokaalid'] = df['monster.lokaalid'].str.replace('NL80_', '')
        df['cleaned_meetwaarde_lokaalid'] = df['meetwaarde.lokaalid'].str.replace('NL80_', '')
        df['locatiecode'] = df['meetobject.lokaalid'].str.replace('NL80_', '')
        df['recordnr_monster'] = df['cleaned_meetwaarde_lokaalid'].rank(method='dense').astype(int)
        
        # Filter to rules with valid validatieregel
        filtered_rules = rules.dropna(subset=['validatieregel'])
        
        if filtered_rules.empty:
            return
        
        # Merge rules with validation rules to get count expectations
        merged = filtered_rules.merge(
            validatie_regels,
            left_on='validatieregel',
            right_index=True,
            how='inner'
        )
        
        # Determine grouping column based on validation rule setting
        group_by_col = 'cleaned_meetwaarde_lokaalid'
        if not validatie_regels.empty:
            group_by_setting = str(validatie_regels.iloc[0].get("group_by", "")).strip()
            if group_by_setting == 'monster.lokaalid':
                group_by_col = 'cleaned_lokaalid'
        
        # Merge with original data
        merged_with_df = merged.merge(
            df,
            left_on='record_id',
            right_on=group_by_col
        )
        
        if merged_with_df.empty:
            return
        
        # Group and count
        grouped = merged_with_df.groupby([
            "validatieregel",
            "databundelcode_x",
            "locatie.code",
            "locatiecode_x"
        ])
        
        for group_key, group_df in grouped:
            aantal_dat = len(group_df)
            aantal_val = group_df['aantal'].iloc[0]
            limiet = group_df['limiet'].iloc[0]
            record_id = group_df['record_id_x'].iloc[0] if 'record_id_x' in group_df.columns else group_df['record_id'].iloc[0]
            
            # Determine record type
            recordnr_monster = group_df['recordnr_monster'].iloc[0] if 'recordnr_monster' in group_df.columns else 0
            soort = "tijdwaarden" if recordnr_monster == 0 else "monsters"
            
            # Check count against limit
            uitvalreden = ""
            if limiet == "<=" and aantal_dat > aantal_val:
                uitvalreden = f"aantal {soort} groter dan verwacht"
            elif limiet == ">=" and aantal_dat < aantal_val:
                uitvalreden = f"aantal {soort} kleiner dan verwacht"
            elif limiet == "=" and aantal_dat != aantal_val:
                uitvalreden = f"aantal {soort} ongelijk aan verwachting"
            
            if uitvalreden:
                self.report.add(
                    section=ValidationSection.COUNT_CHECK,
                    databundelcode=package_name,
                    record_id=record_id,
                    uitvalreden=uitvalreden,
                    informatie=f"aantal datarecords: {aantal_dat}. aantal verwacht: {limiet} {aantal_val}"
                )
    
    def _check_parameters(
        self,
        gdf: gpd.GeoDataFrame,
        package_name: str,
        rules: pd.DataFrame
    ) -> None:
        """
        Check parameter validity.
        
        Validates that parameters in the data match allowed parameters
        defined in the group list for the applicable validation rules.
        """
        validatie_regels = self.ref_data.get_validation_rules(package_name)
        group = self.ref_data.group.copy()
        
        if validatie_regels.empty or rules.empty:
            return
        
        # Prepare data
        df = gdf.copy()
        df['cleaned_lokaalid'] = df['monster.lokaalid'].str.replace('NL80_', '')
        df['cleaned_meetwaarde_lokaalid'] = df['meetwaarde.lokaalid'].str.replace('NL80_', '')
        df['locatiecode'] = df['meetobject.lokaalid'].str.replace('NL80_', '')
        
        # Build parameter column
        df['parameter'] = df.apply(
            lambda row: row['biotaxon.naam'] if pd.notna(row['biotaxon.naam']) and pd.isna(row['parameter.code'])
            else (row['parameter.code'] if pd.notna(row['parameter.code']) and pd.isna(row['biotaxon.naam'])
                  else f"{row['parameter.code']} / {row['biotaxon.naam']}"),
            axis=1
        )
        
        # Explode validation rules by location code
        validatie_regels = validatie_regels.dropna(subset=["locatiecode"])
        validatie_regels["locatiecode"] = validatie_regels["locatiecode"].str.split(";")
        validatie_regels = validatie_regels.explode("locatiecode")
        validatie_regels.index = validatie_regels.index + 2
        
        # Filter rules with uitvalreden in (1, 2, 3) - partial matches
        filtered_rules = rules[rules['uitvalreden'].isin([1, 2, 3])]
        
        if filtered_rules.empty:
            return
        
        # Join data with filtered rules
        merged_with_val = pd.merge(
            df,
            filtered_rules,
            left_on='cleaned_meetwaarde_lokaalid',
            right_on='record_id'
        )
        
        if merged_with_val.empty:
            return
        
        # Reset index and add validatieregel column for join
        validatie_regels_reset = validatie_regels.reset_index().rename(columns={'index': 'validatieregel'})
        
        # Join with validation rules to get group info
        merged_with_regels = pd.merge(
            merged_with_val,
            validatie_regels_reset[['validatieregel', 'groep']],
            on='validatieregel',
            how='inner'
        )
        
        if merged_with_regels.empty:
            return
        
        # Add lowercase group column
        merged_with_regels['val_groep'] = merged_with_regels['groep'].str.lower()
        
        # Filter out rows where parameter is "nan / nan" and val_groep is empty
        filtered_df = merged_with_regels[
            ~((merged_with_regels['parameter'] == "nan / nan") & 
              (merged_with_regels['val_groep'].isna()))
        ]
        
        # Check if parameters exist in the group list
        group_params_lower = set(group['parameter'].str.lower().dropna())
        
        for _, row in filtered_df.iterrows():
            param = str(row['parameter']).lower() if pd.notna(row['parameter']) else ''
            val_groep = row['val_groep'] if pd.notna(row['val_groep']) else ''
            
            # Check if parameter exists in any group
            if param and param != 'nan' and param != 'nan / nan':
                if param not in group_params_lower:
                    record_id = row.get('record_id_x', row.get('record_id', ''))
                    self.report.add(
                        section=ValidationSection.PARAMETER_CHECK,
                        databundelcode=package_name,
                        record_id=record_id,
                        uitvalreden='parameter is ongeldig',
                        informatie=f'parameter "{row["parameter"]}" i.c.m. groep ({val_groep}) uit de validatieregel komt niet voor in de groep-lijst'
                    )
    
    def _check_parameter_aggregates(
        self,
        gdf: gpd.GeoDataFrame,
        package_name: str,
        rules: pd.DataFrame
    ) -> None:
        """
        Check parameter aggregates (verzamelingen).
        
        Validates that when a parameter belongs to a collection (verzameling),
        all required parameters from that collection are present.
        """
        validatie_regels = self.ref_data.get_validation_rules(package_name)
        group = self.ref_data.group.copy()
        
        if validatie_regels.empty or rules.empty:
            return
        
        # Prepare data
        df = gdf.copy()
        df['parameter'] = df.apply(
            lambda row: row['biotaxon.naam'] if pd.notna(row['biotaxon.naam']) and pd.isna(row['parameter.code'])
            else (row['parameter.code'] if pd.notna(row['parameter.code']) and pd.isna(row['biotaxon.naam'])
                  else f"{row['parameter.code']} / {row['biotaxon.naam']}"),
            axis=1
        )
        
        # Adjust validation rules index
        validatie_regels.index = validatie_regels.index + 2
        
        # Filter rules with valid validatieregel
        filtered_rules = rules.dropna(subset=['validatieregel'])
        
        if filtered_rules.empty:
            return
        
        # Merge rules with validation rules
        merged = filtered_rules.merge(
            validatie_regels,
            left_on='validatieregel',
            right_index=True,
            how='inner'
        )
        
        if merged.empty:
            return
        
        # Merge with group data
        merged_with_group = merged.merge(
            group,
            how='left',
            left_on='groep',
            right_on='groep'
        )
        
        # Filter for verzameling records with no errors
        verzamelingen = merged_with_group[
            (merged_with_group['betreftverzameling'] == 1) &
            (merged_with_group['uitvalreden'] == 0)
        ]
        
        if verzamelingen.empty:
            return
        
        # Group by groep and parameter
        verzamelingen = verzamelingen.assign(
            group_lower=verzamelingen['groep'].str.lower(),
            parameter_lower=verzamelingen['parameter_y'].str.lower() if 'parameter_y' in verzamelingen.columns 
                           else verzamelingen['parameter'].str.lower()
        )
        
        grouped = verzamelingen.groupby(['groep', 'parameter_lower']).agg({
            'record_id': 'min',
            'databundelcode_x': 'min'
        }).reset_index()
        
        # Get all parameters in groups (lowercase for comparison)
        group_params = set(group['parameter'].str.lower().dropna())
        
        # Find missing parameters
        for _, row in grouped.iterrows():
            param = row['parameter_lower']
            if pd.notna(param) and param not in group_params:
                self.report.add(
                    section=ValidationSection.PARAMETER_AGGREGATE,
                    databundelcode=package_name,
                    record_id=row['record_id'],
                    uitvalreden='ontbrekende parameter',
                    informatie=f'parameter "{param}" uit groep "{row["groep"]}" niet gevonden'
                )
    
    def _check_fixed_values(self, gdf: gpd.GeoDataFrame, package_name: str) -> None:
        """Check fixed value constraints."""
        df = gdf.copy()
        
        # Kwaliteitsoordeel check
        invalid_mask = ~df['kwaliteitsoordeel.code'].isin(self.ALLOWED_KWALITEITSOORDEEL)
        for _, row in df[invalid_mask].iterrows():
            self.report.add(
                section=ValidationSection.VALUE_CHECK,
                databundelcode=package_name,
                record_id=row['meetwaarde.lokaalid'],
                uitvalreden='vaste waarde ongeldig',
                informatie=f'Kwaliteitsoordeel "{row["kwaliteitsoordeel.code"]}" niet in (00,03,04,25,99)'
            )
        
        # Namespace check
        invalid_mask = df['namespace'] != 'NL80'
        for _, row in df[invalid_mask].iterrows():
            self.report.add(
                section=ValidationSection.VALUE_CHECK,
                databundelcode=package_name,
                record_id=row['meetwaarde.lokaalid'],
                uitvalreden='vaste waarde ongeldig',
                informatie=f'Namespace "{row["namespace"]}" ongelijk aan "NL80"'
            )
        
        # Reference horizontal check
        invalid_mask = ~df['referentiehorizontaal.code'].isin(self.ALLOWED_REFERENTIEHORIZONTAAL)
        for _, row in df[invalid_mask].iterrows():
            self.report.add(
                section=ValidationSection.VALUE_CHECK,
                databundelcode=package_name,
                record_id=row['meetwaarde.lokaalid'],
                uitvalreden='vaste waarde ongeldig',
                informatie=f'Referentiehorizontaal.code "{row["referentiehorizontaal.code"]}" ongelijk aan "EPSG:4258"'
            )
        
        # Analysecompartiment should be empty
        invalid_mask = df['analysecompartiment.code'].notna()
        for _, row in df[invalid_mask].iterrows():
            self.report.add(
                section=ValidationSection.VALUE_CHECK,
                databundelcode=package_name,
                record_id=row['meetwaarde.lokaalid'],
                uitvalreden='vaste waarde ongeldig',
                informatie='analysecompartiment_code is niet leeg'
            )
    
    def _check_rules(self, rules: pd.DataFrame) -> None:
        """Check that all records have a matching validation rule."""
        no_rule = rules[rules['validatieregel'].isna()]
        
        for _, row in no_rule.iterrows():
            self.report.add(
                section=ValidationSection.RULE_CHECK,
                databundelcode=row['databundelcode'],
                record_id=row['record_id'],
                uitvalreden='geen validatieregel',
                informatie='geen enkele validatieregel van toepassing'
            )
    
    def _check_other(self, gdf: gpd.GeoDataFrame, package_name: str) -> None:
        """Run miscellaneous validation checks."""
        df = gdf.copy()
        
        # Both numeric and alphanumeric values missing
        missing_mask = df['numeriekewaarde'].isna() & df['alfanumeriekewaarde'].isna()
        for _, row in df[missing_mask].iterrows():
            self.report.add(
                section=ValidationSection.OTHER_CHECK,
                databundelcode=package_name,
                record_id=row['meetwaarde.lokaalid'],
                uitvalreden='waarde ontbreekt',
                informatie='numerieke EN alfanumerieke waarde zijn leeg'
            )
        
        # Invalid limit symbols
        invalid_mask = df['limietsymbool'].notna() & ~df['limietsymbool'].isin(['<', '>'])
        for _, row in df[invalid_mask].iterrows():
            self.report.add(
                section=ValidationSection.OTHER_CHECK,
                databundelcode=package_name,
                record_id=row['meetwaarde.lokaalid'],
                uitvalreden='limietsymbool ongeldig',
                informatie='limietsymbool dient leeg te zijn of < of >'
            )
    
    def _check_date_range(self, gdf: gpd.GeoDataFrame, package_name: str) -> None:
        """Check that dates fall within valid range."""
        rules = self.ref_data.get_validation_rules(package_name)
        if rules.empty:
            return
        
        min_start = pd.to_datetime(rules['startdatum'], dayfirst=True).min()
        max_end = pd.to_datetime(rules['einddatum'], dayfirst=True).max()
        
        df = gdf.copy()
        df['begindatum'] = pd.to_datetime(df['begindatum'], format='mixed')
        
        out_of_range = ~df['begindatum'].between(min_start, max_end)
        for _, row in df[out_of_range].iterrows():
            self.report.add(
                section=ValidationSection.DATE_RANGE,
                databundelcode=package_name,
                record_id=row['meetwaarde.lokaalid'],
                uitvalreden='datum valt buiten bereik',
                informatie=(
                    f"{row['begindatum'].strftime('%d-%m-%Y')} valt buiten datumbereik "
                    f"validatieregels ({min_start.strftime('%d-%m-%Y')} tm {max_end.strftime('%d-%m-%Y')})"
                )
            )
    
    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------
    
    @staticmethod
    def _get_parameter_value(row: pd.Series) -> Optional[str]:
        """Extract parameter value from row."""
        if pd.notna(row['biotaxon.naam']) and pd.isna(row['parameter.code']):
            return str(row['biotaxon.naam']).lower()
        elif pd.notna(row['parameter.code']) and pd.isna(row['biotaxon.naam']):
            return str(row['parameter.code']).lower()
        return np.nan
    
    @staticmethod
    def _values_match(data_value, rule_value) -> bool:
        """Check if data value matches rule value (handling NaN)."""
        if pd.isna(data_value) and pd.isna(rule_value):
            return True
        if pd.isna(data_value) or pd.isna(rule_value):
            return False
        return str(data_value) == str(rule_value)
    
    @staticmethod
    def _value_in_list(data_value, rule_value: str) -> bool:
        """Check if data value is in semicolon-separated rule value."""
        if pd.isna(data_value) and pd.isna(rule_value):
            return True
        if pd.isna(data_value) or pd.isna(rule_value):
            return False
        return str(data_value) in str(rule_value).split(';')
    
    def _count_mismatches(
        self,
        row: pd.Series,
        rules: pd.DataFrame,
        check_columns: list
    ) -> dict[str, int]:
        """Count how many rules each column value fails to match."""
        counts = {}
        
        for data_col, rule_col, _ in check_columns:
            count = 0
            for _, rule in rules.iterrows():
                both_nan = pd.isna(row.get(data_col)) and pd.isna(rule.get(rule_col))
                matches = str(row.get(data_col, '')) in str(rule.get(rule_col, ''))
                if not both_nan and not matches:
                    count += 1
            counts[data_col] = count
        
        return counts
    
    @staticmethod
    def _location_matches(row: pd.Series, rule: pd.Series) -> bool:
        """Check if location code matches rule."""
        loc_code = row.get('locatie.code', '')
        rule_locs = str(rule.get('locatiecode', ''))
        
        if pd.isna(loc_code) and pd.isna(rule.get('locatiecode')):
            return True
        if pd.isna(loc_code) or pd.isna(rule.get('locatiecode')):
            return False
        return loc_code in rule_locs
