terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Primary provider (us-east-1)
# Uses environment-based auth: AWS_PROFILE, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or instance profile
provider "aws" {
  region = var.aws_region_primary

  default_tags {
    tags = var.tags
  }
}

# Secondary provider for us-west-2 (cross-region tests)
# Uses environment-based auth: AWS_PROFILE, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or instance profile
provider "aws" {
  alias  = "west"
  region = var.aws_region_secondary

  default_tags {
    tags = var.tags
  }
}

# S3 bucket in us-east-1 (primary)
resource "aws_s3_bucket" "east_1a" {
  bucket = "${var.project_prefix}-east-1a"

  tags = {
    Name   = "${var.project_prefix}-east-1a"
    Region = var.aws_region_primary
  }
}

# S3 bucket in us-east-1 (secondary, for cross-bucket tests)
resource "aws_s3_bucket" "east_1b" {
  bucket = "${var.project_prefix}-east-1b"

  tags = {
    Name   = "${var.project_prefix}-east-1b"
    Region = var.aws_region_primary
  }
}

# S3 bucket in us-west-2 (for cross-region tests)
resource "aws_s3_bucket" "west_2" {
  provider = aws.west
  bucket   = "${var.project_prefix}-west-2"

  tags = {
    Name   = "${var.project_prefix}-west-2"
    Region = var.aws_region_secondary
  }
}

# Enable versioning for all buckets (best practice)
resource "aws_s3_bucket_versioning" "east_1a" {
  bucket = aws_s3_bucket.east_1a.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "east_1b" {
  bucket = aws_s3_bucket.east_1b.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "west_2" {
  provider = aws.west
  bucket   = aws_s3_bucket.west_2.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block public access (security best practice)
resource "aws_s3_bucket_public_access_block" "east_1a" {
  bucket = aws_s3_bucket.east_1a.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "east_1b" {
  bucket = aws_s3_bucket.east_1b.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "west_2" {
  provider = aws.west
  bucket   = aws_s3_bucket.west_2.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
