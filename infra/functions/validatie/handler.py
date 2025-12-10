"""AWS Lambda handler for KRM validation."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from s3_functions import delete_file_from_s3, publish_to_sqs, report_databundle, upload_file_to_s3

from config import ValidationConfig
from exporter import GeoPackageExporter, set_criteria
from processor import DataBundleProcessor
from reference_data import ReferenceDataLoader
from reporting import generate_count_report
from validator import KRMValidator


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    AWS Lambda entry point for KRM data bundle validation.
    
    Args:
        event: Lambda event (S3 trigger event or empty for local testing)
        context: Lambda context
        
    Returns:
        Response dict with statusCode, message, and validation results
    """
    # Initialize configuration
    config = ValidationConfig.from_environment()
    
    # Get input parameters
    if config.is_local:
        bucket_name = config.bucket_name
        zip_file_key = "input/WMR_2024_01+Noordzeebenthos+bodemschaaf_tijdkolom_3031.zip"
    else:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        zip_file_key = event['Records'][0]['s3']['object']['key']
    
    try:
        result = process_data_bundle(config, bucket_name, zip_file_key)
        return {
            'statusCode': 200,
            'message': 'Data bundle processed successfully',
            **result
        }
    except Exception as e:
        print(f"Error processing data bundle: {e}")
        return {
            'statusCode': 500,
            'message': str(e)
        }


def process_data_bundle(
    config: ValidationConfig,
    bucket_name: str,
    zip_file_key: str
) -> dict[str, Any]:
    """
    Process a single data bundle.
    
    Args:
        config: Validation configuration
        bucket_name: S3 bucket name
        zip_file_key: Path to ZIP file in S3
        
    Returns:
        Dict with processing results
    """
    # Initialize components
    processor = DataBundleProcessor(config)
    ref_data = ReferenceDataLoader(config)
    
    # Extract data from S3
    csv_content, has_akkoord = processor.extract_from_s3(bucket_name, zip_file_key)
    
    # Convert to GeoDataFrame
    gdf = processor.to_geodataframe(csv_content)
    
    # Get package name
    package_name = processor.extract_package_name(zip_file_key)
    clean_package_name = package_name.replace('+', ' ')
    
    # Delete existing geopackage
    delete_file_from_s3(config.bucket_name, f'geopackages/{package_name}.gpkg')
    
    # Run validation
    validator = KRMValidator(config, ref_data)
    report = validator.validate(gdf, package_name)
    
    # Get the determined rules for reporting
    rules = validator._determine_rules(gdf, clean_package_name)
    
    # Generate and save count report
    count_report_df, count_report_path = generate_count_report(
        config, ref_data, gdf, rules, package_name
    )
    upload_file_to_s3(
        str(count_report_path),
        config.bucket_name,
        f'rapportages/validatielijst_per_locatie_met_aantal_{clean_package_name}.csv'
    )
    
    # Save validation report
    report_path = config.temp_folder / f'{clean_package_name}.csv'
    report.to_csv(report_path)
    upload_file_to_s3(
        str(report_path),
        config.bucket_name,
        f'rapportages/{clean_package_name}.csv'
    )
    
    # Apply criteria and prepare output
    df_with_criteria = set_criteria(gdf, ref_data.validatielijst, package_name)
    
    # Drop columns not needed in output
    drop_cols = ['resultaatdatum', 'namespace', 'analysecompartiment.code']
    df_with_criteria = df_with_criteria.drop(
        columns=[c for c in drop_cols if c in df_with_criteria.columns]
    )
    
    bundel_akkoord = report.is_valid
    
    # Export if valid or has akkoord file
    if bundel_akkoord or has_akkoord:
        _export_geopackage(config, df_with_criteria, package_name)
        
        report_databundle(
            df_with_criteria,
            package_name,
            f"Databundel validatie is: {bundel_akkoord} en akkoord file is: {has_akkoord}"
        )
        
        if not config.is_local:
            _upload_and_notify(config, package_name)
    else:
        report_databundle(
            df_with_criteria,
            package_name,
            f"Databundel validatie is: {bundel_akkoord} en akkoord file is: {has_akkoord}"
        )
    
    return {
        'bundle_valid': bundel_akkoord,
        'has_akkoord': has_akkoord,
        'validation_failures': report.failure_count,
        'failures_by_section': {
            section.value: count 
            for section, count in report.failures_by_section().items()
        }
    }


def _export_geopackage(
    config: ValidationConfig,
    gdf,
    package_name: str
) -> None:
    """Export data to GeoPackage file."""
    exporter = GeoPackageExporter(config)
    gpkg_path = config.temp_folder / 'output.gpkg'
    exporter.export(gdf, gpkg_path)


def _upload_and_notify(config: ValidationConfig, package_name: str) -> None:
    """Upload GeoPackage to S3 and send SQS notification."""
    gpkg_path = config.temp_folder / 'output.gpkg'
    
    upload_file_to_s3(
        str(gpkg_path),
        config.bucket_name,
        f'geopackages/{package_name}.gpkg'
    )
    
    publish_to_sqs(
        queue_url=config.sqs_queue_url,
        message_body="test",
        message_attributes=None,
        message_group_id='example_group_id'
    )


# Allow running directly for local testing
if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(f"Result: {result}")
