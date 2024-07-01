data "aws_caller_identity" "current_account" {}


locals {
  is_prod   = terraform.workspace == "prod" ? true : false
  image_tag = local.is_prod ? "prod" : "dev"
  default_tags = {
    "terraform"   = "true"
    "project"     = "river-ice"
    "environment" = terraform.workspace
  }

  aws_account_id              = data.aws_caller_identity.current_account.account_id
}