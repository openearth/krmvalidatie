terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.31.0"
    }
  }
  backend "s3" {
    bucket = "krmvalidatie-terraform-state"
    key    = "krm-validatie.tfstate"
    region = "eu-west-1"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.default_tags
  }
}
