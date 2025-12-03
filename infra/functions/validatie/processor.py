"""Data bundle processing from S3 ZIP files."""

from __future__ import annotations

import io
import zipfile
from typing import TYPE_CHECKING
from urllib.parse import unquote_plus

import boto3
import geopandas as gpd
import pandas as pd
from shapely import wkt

if TYPE_CHECKING:
    from .config import ValidationConfig


class DataBundleProcessor:
    """Processes data bundles from S3 ZIP files."""
    
    # Column name variations that need normalization
    COLUMN_MAPPINGS = {
        'tijd.utcoffset': 'tijd_utcoffset',
        'begindiepte.m': 'begindiepte_m',
        'einddiepte.m': 'einddiepte_m',
    }
    
    def __init__(self, config: "ValidationConfig"):
        self.config = config
        self.s3 = boto3.client('s3')
    
    def extract_from_s3(
        self, 
        bucket_name: str, 
        zip_file_key: str
    ) -> tuple[pd.DataFrame, bool]:
        """
        Extract CSV from ZIP file in S3.
        
        Args:
            bucket_name: S3 bucket name
            zip_file_key: Key/path to the ZIP file in S3
            
        Returns:
            Tuple of (DataFrame with CSV content, has_akkoord_file boolean)
            
        Raises:
            ValueError: If no CSV file found in ZIP
        """
        decoded_key = unquote_plus(zip_file_key)
        
        # Download ZIP from S3
        zip_obj = self.s3.get_object(Bucket=bucket_name, Key=decoded_key)
        zip_data = zip_obj['Body'].read()
        
        csv_content = None
        has_akkoord = False
        
        with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
            file_list = z.namelist()
            
            # Check for akkoord.txt
            has_akkoord = "akkoord.txt" in file_list
            
            # Find and read first CSV file
            for file_name in file_list:
                if file_name.endswith('.csv'):
                    with z.open(file_name) as csvfile:
                        # Use cp1252 encoding (Windows Western European)
                        with io.TextIOWrapper(csvfile, encoding='cp1252') as textfile:
                            csv_content = pd.read_csv(textfile, delimiter=';')
                            csv_content.columns = csv_content.columns.str.lower().str.strip()
                    break
        
        if csv_content is None or csv_content.empty:
            raise ValueError("No CSV content found in ZIP file")
        
        return csv_content, has_akkoord
    
    def to_geodataframe(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        """
        Convert DataFrame to GeoDataFrame with proper geometry.
        
        Args:
            df: DataFrame with geometriepunt.x and geometriepunt.y columns
            
        Returns:
            GeoDataFrame with point geometry in EPSG:4258
        """
        df = df.copy()
        
        # Normalize column name variations
        df = self._normalize_column_names(df)
        
        # Create WKT geometry column
        df['geom'] = df.apply(
            lambda row: f"POINT({row['geometriepunt.x']} {row['geometriepunt.y']})",
            axis=1
        )
        df['geom'] = df['geom'].apply(wkt.loads)
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry='geom', crs="EPSG:4258")
        gdf.columns = gdf.columns.str.lower()
        
        return gdf
    
    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column name variations to standard names."""
        df = df.copy()
        
        for old_name, new_name in self.COLUMN_MAPPINGS.items():
            if old_name in df.columns and new_name not in df.columns:
                df = df.rename(columns={old_name: new_name})
        
        return df
    
    @staticmethod
    def extract_package_name(zip_file_key: str) -> str:
        """
        Extract package name from ZIP file path.
        
        Args:
            zip_file_key: S3 key or file path
            
        Returns:
            Package name without path and .zip extension
        """
        from pathlib import Path
        return Path(zip_file_key).stem
