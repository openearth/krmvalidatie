"""Reporting utilities for KRM validation."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from .config import ValidationConfig
    from .reference_data import ReferenceDataLoader


class CountReportGenerator:
    """
    Generates detailed count reports (tellingen) for validation.
    
    This creates the 'validatielijst_per_locatie_met_aantal' report
    that shows expected vs actual counts per location and validation rule.
    """
    
    def __init__(self, config: "ValidationConfig", ref_data: "ReferenceDataLoader"):
        self.config = config
        self.ref_data = ref_data
    
    def generate(
        self,
        gdf: pd.DataFrame,
        rules: pd.DataFrame,
        package_name: str
    ) -> pd.DataFrame:
        """
        Generate count report DataFrame.
        
        Args:
            gdf: GeoDataFrame with the data
            rules: DataFrame with determined rules per record
            package_name: Name of the data bundle
            
        Returns:
            DataFrame with count statistics per location/rule combination
        """
        clean_name = package_name.replace('+', ' ')
        validatie_regels = self.ref_data.get_validation_rules_exploded(clean_name)
        
        if validatie_regels.empty or rules.empty:
            return pd.DataFrame()
        
        # Prepare data
        df = gdf.copy()
        df['cleaned_lokaalid'] = df['monster.lokaalid'].str.replace('NL80_', '')
        df['cleaned_meetwaarde_lokaalid'] = df['meetwaarde.lokaalid'].str.replace('NL80_', '')
        df['locatiecode'] = df['meetobject.lokaalid'].str.replace('NL80_', '')
        df['recordnr_monster'] = df['cleaned_meetwaarde_lokaalid'].rank(method='dense').astype(int)
        
        # Filter rules with valid validatieregel
        filtered_rules = rules.dropna(subset=['validatieregel'])
        
        if filtered_rules.empty:
            return pd.DataFrame()
        
        # Merge rules with validation rules
        merged = filtered_rules.merge(
            validatie_regels,
            left_on='validatieregel',
            right_index=True,
            how='inner'
        )
        
        # Determine grouping column
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
            return pd.DataFrame()
        
        # Group and aggregate
        grouped = merged_with_df.groupby([
            "validatieregel",
            "databundelcode_x",
            "locatie.code",
            "locatiecode_x"
        ])
        
        results = []
        for group_key, group_df in grouped:
            aantal_dat = len(group_df)
            aantal_val = group_df['aantal'].iloc[0]
            limiet = group_df['limiet'].iloc[0]
            record_id = group_df.get('record_id_x', group_df.get('record_id', pd.Series(['']))).iloc[0]
            
            # Get validation rule details
            validatieregel_id = int(group_df['validatieregel'].iloc[0])
            matching_rules = validatie_regels[validatie_regels.index == validatieregel_id]
            validatieregel_dict = self._merge_rule_records(matching_rules)
            
            # Determine record type and uitvalreden
            recordnr_monster = group_df.get('recordnr_monster', pd.Series([0])).iloc[0]
            soort = "tijdwaarden" if recordnr_monster == 0 else "monsters"
            
            uitvalreden = ""
            if limiet == "<=" and aantal_dat > aantal_val:
                uitvalreden = f"aantal {soort} groter dan verwacht"
            elif limiet == ">=" and aantal_dat < aantal_val:
                uitvalreden = f"aantal {soort} kleiner dan verwacht"
            elif limiet == "=" and aantal_dat != aantal_val:
                uitvalreden = f"aantal {soort} ongelijk aan verwachting"
            
            results.append({
                'databundelcode': clean_name,
                'record_id': record_id,
                'locatiecode_aantal': group_df['locatie.code'].iloc[0],
                'aantaldat': aantal_dat,
                'limiet': limiet,
                'aantalval': aantal_val,
                'uitvalreden': uitvalreden,
                'recordnrs': '',
                'validatieregel': str(validatieregel_dict)
            })
        
        return pd.DataFrame(results)
    
    @staticmethod
    def _merge_rule_records(matching_rules: pd.DataFrame) -> dict[str, Any]:
        """Merge multiple matching rule records into single dict."""
        merged_dict: dict[str, set] = defaultdict(set)
        
        for record in matching_rules.to_dict(orient='records'):
            for key, value in record.items():
                merged_dict[key].add(value)
        
        result = {}
        for key, value_set in merged_dict.items():
            if len(value_set) == 1:
                result[key] = next(iter(value_set))
            else:
                result[key] = ';'.join(str(v) for v in sorted(value_set, key=str))
        
        return result
    
    def save(
        self,
        report_df: pd.DataFrame,
        package_name: str,
        output_dir: Path | None = None
    ) -> Path:
        """
        Save count report to CSV.
        
        Args:
            report_df: The report DataFrame
            package_name: Package name for filename
            output_dir: Output directory (defaults to config temp folder)
            
        Returns:
            Path to saved file
        """
        if output_dir is None:
            output_dir = self.config.temp_folder
        
        clean_name = package_name.replace('+', ' ')
        filename = f'validatielijst_per_locatie_met_aantal_{clean_name}.csv'
        filepath = output_dir / filename
        
        # Convert validatieregel column to string if needed
        if 'validatieregel' in report_df.columns:
            report_df = report_df.copy()
            report_df['validatieregel'] = report_df['validatieregel'].astype(str)
        
        # Remove duplicates
        report_df = report_df.drop_duplicates()
        report_df.to_csv(filepath, index=False)
        
        return filepath


def generate_count_report(
    config: "ValidationConfig",
    ref_data: "ReferenceDataLoader",
    gdf: pd.DataFrame,
    rules: pd.DataFrame,
    package_name: str
) -> tuple[pd.DataFrame, Path]:
    """
    Convenience function to generate and save count report.
    
    Args:
        config: Validation configuration
        ref_data: Reference data loader
        gdf: GeoDataFrame with data
        rules: Determined rules DataFrame
        package_name: Package name
        
    Returns:
        Tuple of (report DataFrame, saved file path)
    """
    generator = CountReportGenerator(config, ref_data)
    report_df = generator.generate(gdf, rules, package_name)
    filepath = generator.save(report_df, package_name)
    return report_df, filepath
