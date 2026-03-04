"""Configuration settings for KRM validation."""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ValidationConfig:
    """Configuration for validation process."""
    
    # S3 settings
    bucket_name: str = field(
        default_factory=lambda: os.environ.get("KRM_BUCKET_NAME", "krm-validatie-data-dev")
    )

    local_folder: Path = Path(".")
    
    # Local/Lambda environment
    is_local: bool = field(
        default_factory=lambda: os.environ.get("IS_LOCAL", "").lower() in ("true", "1", "yes")
        or os.environ.get("AWS_EXECUTION_ENV") is None
    )

    sqs_queue_url: str = "https://sqs.eu-west-1.amazonaws.com/637423531264/publishToTest.fifo"
    
    # GitHub base URL for reference data
    github_base_url: str = "https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/main/data"
    
    # Validation thresholds
    max_location_distance_m: float = 100.0
    
    @property
    def temp_folder(self) -> Path:
        """Get temporary folder based on environment."""
        if self.is_local:
            return self.local_folder
        return Path("/tmp")
    
    @classmethod
    def from_environment(cls) -> "ValidationConfig":
        """Create config from environment variables."""
        return cls()
