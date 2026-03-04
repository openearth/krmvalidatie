"""
KRM Data Bundle Validation System

Validates Dutch marine data bundles according to KRM (Kaderrichtlijn Mariene Strategie) criteria.
"""

from .config import ValidationConfig
from .report import ValidationReport, ValidationResult, ValidationSection
from .reference_data import ReferenceDataLoader
from .processor import DataBundleProcessor
from .validator import KRMValidator
from .exporter import GeoPackageExporter, set_criteria
from .reporting import CountReportGenerator, generate_count_report
from .handler import lambda_handler
from .s3_functions import upload_file_to_s3, delete_file_from_s3, publish_to_sqs
from .github_functions import get_data_from_github, get_shape_data_from_github

__all__ = [
    # Core classes
    "ValidationConfig",
    "ValidationReport",
    "ValidationResult",
    "ValidationSection",
    "ReferenceDataLoader",
    "DataBundleProcessor",
    "KRMValidator",
    "GeoPackageExporter",
    # Functions
    "set_criteria",
    "CountReportGenerator",
    "generate_count_report",
    "lambda_handler",
    # Utilities
    "upload_file_to_s3",
    "delete_file_from_s3",
    "publish_to_sqs",
    "get_data_from_github",
    "get_shape_data_from_github",
]

__version__ = "2.0.0"

