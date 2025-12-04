"""Configuration settings for KRM validation."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class ValidationConfig:
    """Configuration for validation process."""
    
    bucket_name: str = "krm-validatie-data-prod"
    local_folder: Path = Path(".")
    is_local: bool = True
    sqs_queue_url: str = "https://sqs.eu-west-1.amazonaws.com/637423531264/publishToTest.fifo"
    
    # GitHub base URL for reference data
    github_base_url: str = "https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data"
    
    # Validation thresholds
    max_location_distance_m: float = 100.0
    
    @property
    def temp_folder(self) -> Path:
        """Get temporary folder based on environment."""
        return self.local_folder if self.is_local else Path("/tmp")
    
    @classmethod
    def from_environment(cls, **overrides) -> "ValidationConfig":
        """Create config detecting local vs Lambda environment."""
        import os
        is_local = os.environ.get('AWS_EXECUTION_ENV') is None
        return cls(is_local=is_local, **overrides)
