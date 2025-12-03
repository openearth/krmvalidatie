.PHONY: help build run test clean lambda-build lambda-run logs shell

# Default target
help:
	@echo "KRM Validator - Docker Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build        Build the Docker image"
	@echo "  run          Run the validator locally"
	@echo "  test         Run tests in Docker"
	@echo "  shell        Open a shell in the container"
	@echo "  logs         Show container logs"
	@echo "  clean        Remove containers and images"
	@echo ""
	@echo "Lambda:"
	@echo "  lambda-build Build Lambda-compatible image"
	@echo "  lambda-run   Run Lambda locally (port 9000)"
	@echo "  lambda-test  Test Lambda endpoint"
	@echo ""
	@echo "LocalStack:"
	@echo "  localstack   Start LocalStack (S3, SQS emulation)"
	@echo "  setup-local  Create local S3 buckets"

# =============================================================================
# Build targets
# =============================================================================

build:
	docker compose build validator

lambda-build:
	docker compose build lambda

build-all:
	docker compose build

# =============================================================================
# Run targets
# =============================================================================

run:
	docker compose up validator

run-detached:
	docker compose up -d validator

lambda-run:
	docker compose up lambda

# Test Lambda endpoint locally
lambda-test:
	@echo "Testing Lambda endpoint..."
	curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
		-d '{"Records": [{"s3": {"bucket": {"name": "krm-validatie-data-prod"}, "object": {"key": "input/test.zip"}}}]}'

# =============================================================================
# Development targets
# =============================================================================

shell:
	docker compose run --rm validator /bin/bash

test:
	docker compose run --rm test

test-verbose:
	docker compose run --rm test python -m pytest tests/ -v --tb=long

logs:
	docker compose logs -f validator

# =============================================================================
# LocalStack targets
# =============================================================================

localstack:
	docker compose up -d localstack
	@echo "Waiting for LocalStack to be ready..."
	@sleep 5
	@echo "LocalStack is ready at http://localhost:4566"

setup-local: localstack
	@echo "Creating S3 buckets..."
	aws --endpoint-url=http://localhost:4566 s3 mb s3://krm-validatie-data-prod || true
	aws --endpoint-url=http://localhost:4566 s3 mb s3://krm-validatie-data-test || true
	@echo "Creating SQS queue..."
	aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name publishToTest.fifo --attributes FifoQueue=true || true
	@echo "Local AWS resources created!"

# =============================================================================
# Cleanup targets
# =============================================================================

stop:
	docker compose down

clean:
	docker compose down --rmi local --volumes --remove-orphans
	docker system prune -f

clean-all:
	docker compose down --rmi all --volumes --remove-orphans
	docker system prune -af

# =============================================================================
# Deployment targets
# =============================================================================

# Build and push to ECR
ecr-login:
	aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.eu-west-1.amazonaws.com

ecr-push: lambda-build
	docker tag krm_validation-lambda:latest $(AWS_ACCOUNT_ID).dkr.ecr.eu-west-1.amazonaws.com/krm-validator:latest
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.eu-west-1.amazonaws.com/krm-validator:latest
