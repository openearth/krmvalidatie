This repository contains Terraform templates for deploying infrastructure on AWS. Follow the steps below to set up your workspace, customize the templates, and deploy your infrastructure.

## Prerequisites

Before you begin, ensure you have the following tools installed:

- [Terraform](https://www.terraform.io/downloads.html) (v1.0 or later)
- [AWS CLI](https://aws.amazon.com/cli/) (configured with appropriate AWS credentials)
- [Git](https://git-scm.com/)

## Repository Structure

- `main.tf`: Main configuration file for Terraform resources
- `variables.tf`: Variables used in the Terraform configuration
- `outputs.tf`: Outputs for the Terraform configuration
- `provider.tf`: Configuration for the AWS provider
- `README.md`: This file

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/openearth/krmvalidatie
```

### 2. Configure AWS Credentials

Ensure your AWS CLI is configured with the necessary credentials. You can do this by running:

```bash
aws configure
```

Follow the prompts to enter your AWS Access Key, Secret Key, region, and output format.

### 3. Initialize Terraform

Initialize your Terraform workspace. This will download the necessary provider plugins.

```bash
terraform init
```

### 4. Customize Variables

Edit the variables.tf file to customize the variables according to your requirements. For example:

```bash
variable "aws_region" {
  description = "The AWS region to deploy in"
  default     = "eu-west-1"
}
```

### 5. Plan the Deployment

Run the following command to see what Terraform will do before actually applying the changes:

```bash
terraform plan
```

Review the output to ensure that the resources to be created/modified/deleted match your expectations.

### 6. Apply the Deployment

Apply the Terraform configuration to deploy the resources:

```bash
terraform apply
```

Type yes when prompted to confirm the deployment.

### 7. Access the Outputs

After the deployment is complete, you can view the outputs defined in outputs.tf:

```bash
terraform output
```

## Managing Workspaces

Terraform workspaces allow you to manage multiple environments (e.g., dev, staging, production) with a single configuration.

### Create a New Workspace

```bash
terraform workspace new <workspace_name>
```

### List Workspaces

```bash
terraform workspace list
```

### Select a Workspace

```bash
terraform workspace select <workspace_name>
```

### Delete a Workspace

```bash
terraform workspace delete <workspace_name>
```

Note: You cannot delete a workspace if it is currently selected. Switch to another workspace first.

### Cleaning Up

To destroy the resources created by Terraform:

```bash
terraform destroy
```

Type yes when prompted to confirm the destruction.


# Creating and Publishing AWS Lambda Layers

This guide explains how to create and publish AWS Lambda layers, using GeoPandas as an example.

## What is a Lambda Layer?

A Lambda layer is a ZIP archive that contains libraries, custom runtimes, or other dependencies. Layers allow you to keep your deployment package small and share code across multiple Lambda functions.

## Size Limitations

**Important size constraints for Lambda layers:**

- **Unzipped layer size**: Maximum 250 MB per layer
- **Total unzipped size** (function + all layers): Maximum 250 MB
- **Zipped deployment package**: Maximum 50 MB (for direct upload to Lambda console)
- **Zipped deployment package via S3**: Maximum 250 MB
- **Maximum layers per function**: 5 layers

## Prerequisites

- AWS CLI configured with appropriate credentials
- Docker installed (recommended for consistent builds)
- Python 3.x matching your Lambda runtime version

## Creating a Lambda Layer with GeoPandas

### Option 1: Using Docker (Recommended)

This ensures compatibility with the Lambda execution environment.
```bash
# Create a directory for your layer
mkdir geopandas-layer
cd geopandas-layer

# Create a Dockerfile
cat > Dockerfile << 'EOF'
FROM public.ecr.aws/lambda/python:3.11

RUN pip install geopandas -t /asset/python/

CMD ["echo", "Layer built successfully"]
EOF

# Build the Docker image
docker build -t geopandas-layer .

# Extract the layer contents
docker run --rm -v $(pwd):/output geopandas-layer \
  sh -c "cp -r /asset /output/"

# Create the zip file
cd asset
zip -r ../geopandas-layer.zip .
cd ..
```

### Option 2: Using pip with --platform flag
```bash
# Create the directory structure
mkdir -p geopandas-layer/python

# Install packages for the Lambda runtime
pip install geopandas \
  --platform manylinux2014_x86_64 \
  --target geopandas-layer/python \
  --only-binary=:all: \
  --python-version 3.11

# Create the zip file
cd geopandas-layer
zip -r ../geopandas-layer.zip .
cd ..
```

### Option 3: Manual build with virtual environment
```bash
# Create a virtual environment
python3 -m venv layer-env
source layer-env/bin/activate

# Install packages
pip install geopandas

# Copy to layer structure
mkdir -p geopandas-layer/python
cp -r layer-env/lib/python3.11/site-packages/* geopandas-layer/python/

# Create zip file
cd geopandas-layer
zip -r ../geopandas-layer.zip .
cd ..

# Clean up
deactivate
rm -rf layer-env
```

## Publishing the Layer to AWS

### Using AWS CLI
```bash
# Publish the layer
aws lambda publish-layer-version \
  --layer-name geopandas-layer \
  --description "GeoPandas and dependencies for geospatial processing" \
  --zip-file fileb://geopandas-layer.zip \
  --compatible-runtimes python3.11 python3.10 \
  --compatible-architectures x86_64

# The command returns a LayerVersionArn - save this for later use
```

### Using AWS Console

1. Navigate to AWS Lambda → Layers
2. Click "Create layer"
3. Enter layer name and description
4. Upload the ZIP file (or provide S3 URL if >50 MB)
5. Select compatible runtimes and architectures
6. Click "Create"

## Using the Layer in a Lambda Function

### Via AWS Console

1. Open your Lambda function
2. Scroll to "Layers" section
3. Click "Add a layer"
4. Select "Custom layers"
5. Choose your layer and version
6. Click "Add"

### Via AWS CLI
```bash
aws lambda update-function-configuration \
  --function-name my-function \
  --layers arn:aws:lambda:eu-west-1:123456789012:layer:geopandas-layer:1
```

### In your Lambda function code
```python
import geopandas as gpd
import pandas as pd

def lambda_handler(event, context):
    # Your GeoPandas code here
    gdf = gpd.read_file('s3://bucket/data.geojson')
    return {
        'statusCode': 200,
        'body': f'Processed {len(gdf)} features'
    }
```

## Tips for Managing Layer Size

GeoPandas and its dependencies (GDAL, GEOS, PROJ, etc.) can be quite large. Here are strategies to stay within limits:

### 1. Use minimal installations
```bash
pip install geopandas --no-deps
pip install pandas shapely pyproj fiona  # Only essential dependencies
```

### 2. Remove unnecessary files
```bash
# Remove tests, documentation, and caches
find geopandas-layer/python -type d -name "tests" -exec rm -rf {} +
find geopandas-layer/python -type d -name "__pycache__" -exec rm -rf {} +
find geopandas-layer/python -name "*.pyc" -delete
find geopandas-layer/python -name "*.pyo" -delete
```

### 3. Consider splitting into multiple layers
```bash
# Layer 1: GDAL and core geospatial libraries
# Layer 2: GeoPandas and data manipulation
# Layer 3: Additional utilities
```

### 4. Use pre-built layers

Consider using existing public layers or AWS-managed layers when available:
- AWS Data Wrangler includes geospatial capabilities
- Community-maintained geospatial layers

## Checking Layer Size
```bash
# Check unzipped size
unzip -l geopandas-layer.zip | tail -1

# Check zipped size
ls -lh geopandas-layer.zip

# Detailed breakdown
du -sh geopandas-layer/python/*
```

## Version Management

### Update existing layer
```bash
# Publish new version
aws lambda publish-layer-version \
  --layer-name geopandas-layer \
  --zip-file fileb://geopandas-layer-v2.zip \
  --compatible-runtimes python3.11

# List all versions
aws lambda list-layer-versions --layer-name geopandas-layer
```

### Delete old versions
```bash
aws lambda delete-layer-version \
  --layer-name geopandas-layer \
  --version-number 1
```

## Sharing Layers

### Make layer public
```bash
aws lambda add-layer-version-permission \
  --layer-name geopandas-layer \
  --version-number 1 \
  --statement-id public-access \
  --action lambda:GetLayerVersion \
  --principal '*'
```

### Share with specific accounts
```bash
aws lambda add-layer-version-permission \
  --layer-name geopandas-layer \
  --version-number 1 \
  --statement-id account-access \
  --action lambda:GetLayerVersion \
  --principal 123456789012
```

## Troubleshooting

### Import errors

Ensure the directory structure is correct:
```
geopandas-layer.zip
└── python/
    ├── geopandas/
    ├── pandas/
    └── ...
```

### Size exceeded

If your layer exceeds 250 MB unzipped, consider:
- Removing unnecessary dependencies
- Splitting into multiple layers
- Using Lambda container images instead (up to 10 GB)
- Downloading dependencies at runtime from S3

### Compatibility issues

Always build layers using the same Python version and architecture as your Lambda runtime. Use Docker with the official Lambda base images for best results.

## Additional Resources

- [AWS Lambda Layers Documentation](https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html)
- [AWS Lambda Runtimes](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html)
- [GeoPandas Documentation](https://geopandas.org/)


## Contributing

If you find a bug or have a feature request, please open an issue or submit a pull request. Contributions are welcome!

## License

This project is licensed under the MIT License - see the LICENSE file for details.