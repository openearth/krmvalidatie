# KRM Validator Dockerfile
# Multi-stage build for smaller final image

# =============================================================================
# Stage 1: Builder - Install dependencies and build wheel
# =============================================================================
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables for building
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install Python dependencies first (for better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir build wheel \
    && pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# Copy source code (matches your structure: functions/validatie/*.py)
COPY functions/validatie/*.py /build/krm_validator/
RUN touch /build/krm_validator/__init__.py

# =============================================================================
# Stage 2: Runtime - Minimal image with only runtime dependencies
# =============================================================================
FROM python:3.11-slim AS runtime

WORKDIR /app

# Install runtime dependencies for GDAL/GEOS
# Package names for Debian Trixie (python:3.11-slim base)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal36 \
    libgeos-c1t64 \
    libproj25 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser

# Copy wheels from builder and install
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels

# Copy application code as a package
COPY --from=builder /build/krm_validator /app/krm_validator/

# Create directories for data
RUN mkdir -p /app/data /app/output /tmp \
    && chown -R appuser:appuser /app /tmp

# Switch to non-root user
USER appuser

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV AWS_DEFAULT_REGION=eu-west-1

# Default command
CMD ["python", "-m", "krm_validator.handler"]


# =============================================================================
# Stage 3: Lambda - AWS Lambda compatible image
# =============================================================================
FROM public.ecr.aws/lambda/python:3.11 AS lambda

# Install GDAL dependencies
RUN yum install -y \
    gdal \
    gdal-devel \
    geos \
    geos-devel \
    proj \
    proj-devel \
    && yum clean all

# Copy wheels from builder and install
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels

# Copy application code as a package
COPY --from=builder /build/krm_validator ${LAMBDA_TASK_ROOT}/

# Set the Lambda handler
CMD ["handler.lambda_handler"]