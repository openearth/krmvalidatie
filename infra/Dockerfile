# KRM Validator Dockerfile
# Multi-stage build for smaller final image

# =============================================================================
# Stage 1: Builder - Install dependencies and build wheel
# =============================================================================
FROM python:3.11-slim as builder

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
COPY pyproject.toml .
RUN pip install --no-cache-dir build wheel \
    && pip wheel --no-cache-dir --wheel-dir /wheels \
    boto3 \
    geopandas \
    numpy \
    pandas \
    shapely \
    pyproj \
    fiona

# Copy source and build the package
COPY src/ src/
COPY README.md .
RUN pip wheel --no-cache-dir --wheel-dir /wheels .


# =============================================================================
# Stage 2: Runtime - Minimal image with only runtime dependencies
# =============================================================================
FROM python:3.11-slim as runtime

WORKDIR /app

# Install runtime dependencies for GDAL/GEOS
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal32 \
    libgeos-c1v5 \
    libproj25 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser

# Copy wheels from builder and install
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels

# Copy application code
COPY src/ src/
COPY pyproject.toml .

# Install the package in editable mode (or regular install)
RUN pip install --no-cache-dir -e .

# Create directories for data
RUN mkdir -p /app/data /app/output /tmp \
    && chown -R appuser:appuser /app /tmp

# Switch to non-root user
USER appuser

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV AWS_DEFAULT_REGION=eu-west-1

# Default command - can be overridden
CMD ["python", "-m", "krm_validator.handler"]


# =============================================================================
# Stage 3: Lambda - AWS Lambda compatible image
# =============================================================================
FROM public.ecr.aws/lambda/python:3.11 as lambda

# Install GDAL dependencies
RUN yum install -y \
    gdal \
    gdal-devel \
    geos \
    geos-devel \
    proj \
    proj-devel \
    && yum clean all

# Copy wheels from builder
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels

# Copy application code
COPY src/ ${LAMBDA_TASK_ROOT}/src/
COPY pyproject.toml ${LAMBDA_TASK_ROOT}/

# Install package
WORKDIR ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -e .

# Set the Lambda handler
CMD ["krm_validator.handler.lambda_handler"]
