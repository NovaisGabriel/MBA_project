variable "region_id" {
  default = "us-east-2"
}

variable "prefix" {
  default = "prefix"
}

variable "account" {
  default = 715036709715
}

# Prefix configuration and project common tags
locals {
  prefix = "${var.prefix}-${terraform.workspace}"
  common_tags = {
    Project      = "Datalake"
    ManagedBy    = "Terraform"
    Department   = "systems",
    Provider     = "Gabriel",
    Owner        = "Data Engineering"
    BusinessUnit = "Data"
    Billing      = "Infrastructure"
    Environment  = terraform.workspace
    UserEmail    = "gabrieel.noavsi@gmail.com"
  }
}

variable "bucket_names" {
  description = "Create S3 buckets with these names"
  type        = list(string)
  default = [
    "landing-zone",
    "processing-zone",
    "delivery-zone"
  ]
}

variable "database_names" {
  description = "Create databases with these names"
  type        = list(string)
  default = [
    #landing-zone
    "dl_landing_zone",
    "dl_processing_zone",
    "dl_delivery_zone"
  ]
}

variable "bucket_paths" {
  description = "Paths to S3 bucket used by the crawler"
  type        = list(string)
  default = [
    "s3://landing-zone-715036709715",
    "s3://processing-zone-715036709715",
    "s3://delivery-zone-715036709715"
  ]
}