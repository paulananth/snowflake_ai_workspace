terraform {
  required_version = ">= 1.7"  # 1.7+ required for mock_provider in terraform test

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment to store Terraform state remotely in S3 (recommended for teams).
  # Create the state bucket manually first, then run `terraform init`.
  #
  # backend "s3" {
  #   bucket  = "your-terraform-state-bucket"
  #   key     = "sec-edgar/aws/terraform.tfstate"
  #   region  = "us-east-1"
  #   encrypt = true
  # }
}

provider "aws" {
  region = var.aws_region
}

# Resolve the calling identity (account ID used throughout for ARN construction)
data "aws_caller_identity" "current" {}

# All route tables in the VPC — attached to the S3 Gateway endpoint
data "aws_route_tables" "vpc" {
  vpc_id = var.vpc_id
}
