# Backend configuration require a AWS storage bucket.
terraform {
  backend "s3" {
    bucket = "s3://terraform-logs-gabriel/logs/"
    key    = "state/terraform.tfstate"
    region = "us-east-1"
  }
}
