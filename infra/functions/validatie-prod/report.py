"""Validation report structures and export functionality."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


class ValidationSection(Enum):
    """Validation check categories."""
    
    GEO_CONTROL = "Geo controle"
    COLUMN_CHECK = "Verplichte kolommen controle"
    COLUMN_VALUE = "Kolomwaarde controle"
    COUNT_CHECK = "Aantal controle"
    PARAMETER_CHECK = "Parameter controle"
    PARAMETER_AGGREGATE = "Parameter verzameling controle"
    VALUE_CHECK = "Vaste waarden controle"
    RULE_CHECK = "Regel controle"
    OTHER_CHECK = "Overige controle"
    DATE_RANGE = "Datumbereik controle"


@dataclass
class ValidationResult:
    """Single validation failure record."""
    
    section: ValidationSection
    databundelcode: str
    record_id: str
    uitvalreden: str
    informatie: str


@dataclass
class ValidationReport:
    """Collection of validation results with export capabilities."""
    
    results: list[ValidationResult] = field(default_factory=list)
    
    def add(
        self,
        section: ValidationSection,
        databundelcode: str,
        record_id: str,
        uitvalreden: str,
        informatie: str,
    ) -> None:
        """Add a validation failure."""
        self.results.append(
            ValidationResult(
                section=section,
                databundelcode=databundelcode,
                record_id=self._clean_record_id(record_id),
                uitvalreden=uitvalreden,
                informatie=informatie,
            )
        )
    
    def add_many(self, section: ValidationSection, df: "pd.DataFrame") -> None:
        """
        Add multiple validation failures from a DataFrame.
        
        DataFrame must contain columns: databundelcode, record_id, uitvalreden, informatie
        """
        required_cols = {'databundelcode', 'record_id', 'uitvalreden', 'informatie'}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"DataFrame must contain columns: {required_cols}")
        
        for _, row in df.iterrows():
            self.add(
                section=section,
                databundelcode=row['databundelcode'],
                record_id=row['record_id'],
                uitvalreden=row['uitvalreden'],
                informatie=row['informatie'],
            )
    
    @staticmethod
    def _clean_record_id(record_id: str) -> str:
        """Remove NL80_ prefix from record ID."""
        if isinstance(record_id, str):
            return record_id.replace('NL80_', '')
        return str(record_id)
    
    @property
    def is_valid(self) -> bool:
        """Check if bundle passed all validations (no failures)."""
        return len(self.results) == 0
    
    @property
    def failure_count(self) -> int:
        """Number of validation failures."""
        return len(self.results)
    
    def failures_by_section(self) -> dict[ValidationSection, int]:
        """Count failures grouped by section."""
        counts: dict[ValidationSection, int] = {}
        for result in self.results:
            counts[result.section] = counts.get(result.section, 0) + 1
        return counts
    
    def to_csv(self, filepath: Path) -> None:
        """Export report to CSV file."""
        headers = ["Section", "Databundelcode", "Record ID", "Uitvalreden", "Informatie"]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for result in self.results:
                writer.writerow([
                    result.section.value,
                    result.databundelcode,
                    result.record_id,
                    result.uitvalreden,
                    result.informatie,
                ])
    
    def to_dataframe(self) -> "pd.DataFrame":
        """Convert report to pandas DataFrame."""
        import pandas as pd
        
        return pd.DataFrame([
            {
                'section': r.section.value,
                'databundelcode': r.databundelcode,
                'record_id': r.record_id,
                'uitvalreden': r.uitvalreden,
                'informatie': r.informatie,
            }
            for r in self.results
        ])
