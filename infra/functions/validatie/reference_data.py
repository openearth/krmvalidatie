"""Reference data loading and caching from GitHub."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import geopandas as gpd
import pandas as pd

from github_functions import get_data_from_github, get_shape_data_from_github

if TYPE_CHECKING:
    from .config import ValidationConfig


class ReferenceDataLoader:
    """
    Loads and caches reference data from GitHub.
    
    Data is loaded lazily on first access and cached for subsequent uses.
    """
    
    def __init__(self, config: "ValidationConfig"):
        self.config = config
        self._base_url = config.github_base_url
        
        # Cached data
        self._validatielijst: Optional[pd.DataFrame] = None
        self._group: Optional[pd.DataFrame] = None
        self._column_definition: Optional[pd.DataFrame] = None
        self._location_gdf: Optional[gpd.GeoDataFrame] = None
    
    @property
    def validatielijst(self) -> pd.DataFrame:
        """Get validation rules list."""
        if self._validatielijst is None:
            self._validatielijst = get_data_from_github(
                f"{self._base_url}/validatielijst.csv"
            )
        return self._validatielijst
    
    @property
    def group(self) -> pd.DataFrame:
        """Get group definitions."""
        if self._group is None:
            self._group = get_data_from_github(f"{self._base_url}/groep.csv")
        return self._group
    
    @property
    def column_definition(self) -> pd.DataFrame:
        """Get column definitions."""
        if self._column_definition is None:
            self._column_definition = get_data_from_github(
                f"{self._base_url}/kolomdefinitie.csv"
            )
        return self._column_definition
    
    @property
    def location_gdf(self) -> gpd.GeoDataFrame:
        """Get combined location GeoDataFrame (points and polygons)."""
        if self._location_gdf is None:
            self._location_gdf = self._load_location_shapefiles()
        return self._location_gdf
    
    @property
    def location_identifiers(self) -> set[str]:
        """Get set of valid location identifiers (MPNIDENT)."""
        return set(self.location_gdf['MPNIDENT'].values)
    
    def _load_location_shapefiles(self) -> gpd.GeoDataFrame:
        """Load and combine point and polygon location shapefiles."""
        local_folder = str(self.config.temp_folder)
        base_url = f"{self._base_url}/KRM_locatiedetails"
        
        # Download all shapefile components for both point and polygon files
        shapefile_extensions = ['.shp', '.shx', '.prj', '.dbf', '.cpg']
        
        for prefix in ['KRM2_P', 'KRM2_V']:
            for ext in shapefile_extensions:
                filename = f"{prefix}{ext}"
                get_shape_data_from_github(
                    f"{base_url}/{filename}", 
                    filename, 
                    local_folder
                )
        
        # Load shapefiles
        gdf_points = gpd.read_file(self.config.temp_folder / 'KRM2_P.shp')
        gdf_polygons = gpd.read_file(self.config.temp_folder / 'KRM2_V.shp')
        
        # Combine into single GeoDataFrame
        combined = pd.concat([gdf_points, gdf_polygons], ignore_index=True)
        return gpd.GeoDataFrame(combined, geometry='geometry')
    
    def get_validation_rules(self, package_name: str) -> pd.DataFrame:
        """
        Get validation rules filtered for a specific package.
        
        Args:
            package_name: The data bundle code (with or without '+' characters)
            
        Returns:
            DataFrame of matching validation rules
        """
        clean_name = package_name.replace('+', ' ')
        return self.validatielijst[
            self.validatielijst['databundelcode'].apply(
                lambda x: clean_name.startswith(x)
            )
        ].copy()
    
    def get_validation_rules_exploded(self, package_name: str) -> pd.DataFrame:
        """
        Get validation rules with location codes exploded into separate rows.
        
        Args:
            package_name: The data bundle code
            
        Returns:
            DataFrame with one row per location code per rule
        """
        rules = self.get_validation_rules(package_name)
        
        if rules.empty:
            return rules
        
        # Split semicolon-separated location codes into separate rows
        rules = rules.copy()
        rules["locatiecode"] = rules["locatiecode"].str.split(";")
        rules = rules.explode("locatiecode")
        
        # Convert dates
        rules['startdatum'] = pd.to_datetime(
            rules['startdatum'], errors='coerce', dayfirst=True
        )
        rules['einddatum'] = pd.to_datetime(
            rules['einddatum'], errors='coerce', dayfirst=True
        )
        
        # Adjust index to match Excel row numbers (for debugging)
        rules.index = rules.index + 2
        
        return rules
    
    def get_groups_for_rules(self, package_name: str) -> pd.DataFrame:
        """Get group data filtered to groups used in validation rules."""
        rules = self.get_validation_rules(package_name)
        return self.group[self.group['groep'].isin(rules['groep'])].copy()
    
    def clear_cache(self) -> None:
        """Clear all cached data (useful for testing or memory management)."""
        self._validatielijst = None
        self._group = None
        self._column_definition = None
        self._location_gdf = None
