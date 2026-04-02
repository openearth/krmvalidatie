"""GeoPackage export functionality."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd
import pandas as pd

if TYPE_CHECKING:
    from config import ValidationConfig


class GeoPackageExporter:
    """Exports validated data to GeoPackage format."""
    
    # Data type mappings for GeoPackage export
    DTYPE_MAPPINGS = {
        "meetobject.lokaalid": 'str',
        "monster.lokaalid": 'str',
        "meetwaarde.lokaalid": 'str',
        "monstercompartiment.code": 'str',
        "begindatum": 'str',
        "begintijd": 'str',
        "einddatum": 'str',
        "eindtijd": 'str',
        "tijd_utcoffset": 'str',
        "typering.code": 'str',
        "grootheid.code": 'str',
        "parameter.code": 'str',
        "parameter.omschrijving": 'str',
        "biotaxon.naam": 'str',
        "eenheid.code": 'str',
        "hoedanigheid.code": 'str',
        "waardebewerkingsmethode.code": 'str',
        "limietsymbool": 'str',
        "numeriekewaarde": 'float',
        "alfanumeriekewaarde": 'str',
        "kwaliteitsoordeel.code": 'str',
        "orgaan.code": 'str',
        "organisme.naam": 'str',
        "bemonsteringsapparaat.omschrijving": 'str',
        "geometriepunt.x": 'float',
        "geometriepunt.y": 'float',
        "referentiehorizontaal.code": 'str',
        "begindiepte_m": 'str',
        "einddiepte_m": 'str',
        "referentievlak.code": 'str',
        "bemonsteringsmethode.code": 'str',
        "bemonsteringsmethode.codespace": 'str',
        "waardebepalingstechniek.code": 'str',
        "monprog.naam": 'str',
        "krmcriterium": 'str',
        "meetobject.namespace": 'str',
        "levensstadium.code": 'str',
        "lengteklasse.code": 'str',
        "geslacht.code": 'str',
        "verschijningsvorm.code": 'str',
        "levensvorm.code": 'str',
        "waardebepalingsmethode.code": 'str',
        "geom": 'object',
    }
    
    DEFAULT_LAYER_NAME = 'krm_actuele_dataset'
    
    def __init__(self, config: "ValidationConfig"):
        self.config = config
    
    def export(
        self,
        gdf: gpd.GeoDataFrame,
        filepath: Path,
        layer_name: str = DEFAULT_LAYER_NAME
    ) -> None:
        """
        Export GeoDataFrame to GeoPackage.
        
        Args:
            gdf: GeoDataFrame to export
            filepath: Output file path
            layer_name: Name of the layer in the GeoPackage
        """
        gdf = gdf.copy()
        
        # Apply type conversions
        for column, dtype in self.DTYPE_MAPPINGS.items():
            if column in gdf.columns:
                try:
                    gdf[column] = gdf[column].astype(dtype)
                except (ValueError, TypeError):
                    # Keep original type if conversion fails
                    pass
        
        gdf.to_file(filepath, layer=layer_name, driver='GPKG')


def set_criteria(
    df: pd.DataFrame,
    validatielijst: pd.DataFrame,
    package_name: str
) -> pd.DataFrame:
    """
    Duplicate records for each applicable KRM criterion.
    
    Args:
        df: Original DataFrame
        validatielijst: Validation rules DataFrame
        package_name: Data bundle name
        
    Returns:
        DataFrame with records duplicated for each criterion
    """
    clean_name = package_name.replace('+', ' ')
    
    # Get matching validation rules
    validatie_regels = validatielijst[
        validatielijst['databundelcode'].apply(lambda x: clean_name.startswith(x))
    ]
    
    if validatie_regels.empty:
        return df
    
    # Get criteria string and split
    criteria = validatie_regels['criteria'].values[0]
    criteria_list = criteria.split(';')
    
    # Duplicate records for each criterion
    duplicated_dfs = []
    for criterium in criteria_list:
        temp_df = df.copy()
        temp_df['krmcriterium'] = f"ANSNL-{criterium}"
        duplicated_dfs.append(temp_df)
    
    return pd.concat(duplicated_dfs, ignore_index=True)
