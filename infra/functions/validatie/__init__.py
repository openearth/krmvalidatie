"""
KRM Data Bundle Validation System

Validates Dutch marine data bundles according to KRM (Kaderrichtlijn Mariene Strategie) criteria.
"""

from config import ValidationConfig
from report import ValidationReport, ValidationResult, ValidationSection
from reference_data import ReferenceDataLoader
from processor import DataBundleProcessor
from validator import KRMValidator
from exporter import GeoPackageExporter, set_criteria
from reporting import CountReportGenerator, generate_count_report
from handler import lambda_handler

__all__ = [
    "ValidationConfig",
    "ValidationReport",
    "ValidationResult", 
    "ValidationSection",
    "ReferenceDataLoader",
    "DataBundleProcessor",
    "KRMValidator",
    "GeoPackageExporter",
    "set_criteria",
    "CountReportGenerator",
    "generate_count_report",
    "lambda_handler",
]

__version__ = "2.0.0"
