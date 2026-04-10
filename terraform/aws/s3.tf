# ---------------------------------------------------------------------------
# S3 Bucket — SEC EDGAR bronze Parquet data
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "bronze" {
  bucket = var.s3_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# S3 VPC Gateway Endpoint — free; keeps S3 traffic inside the AWS network
# and avoids NAT Gateway data-processing charges.
# ---------------------------------------------------------------------------

resource "aws_vpc_endpoint" "s3_gateway" {
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"

  # Attach to every route table in the VPC so all subnets benefit
  route_table_ids = data.aws_route_tables.vpc.ids

  tags = merge(var.tags, { Name = "sec-edgar-s3-gateway" })
}
