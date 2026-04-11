# terraform/aws/.tflint.hcl
# tflint configuration for the SEC EDGAR Bronze AWS Terraform module.
#
# Install tflint:   brew install tflint  (macOS)
#                   https://github.com/terraform-linters/tflint#installation
# Install plugins:  tflint --init  (run once from terraform/aws/)
# Run checks:       tflint

plugin "aws" {
  enabled = true
  version = "0.34.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}

# Enforce explicit version constraints in required_providers
rule "terraform_required_providers" {
  enabled = true
}

# Enforce terraform required_version is declared
rule "terraform_required_version" {
  enabled = true
}

# Disallow deprecated interpolation syntax (${var.x} in strings where not needed)
rule "terraform_deprecated_interpolation" {
  enabled = true
}

# Warn on empty variable descriptions
rule "terraform_documented_variables" {
  enabled = true
}

# Warn on empty output descriptions
rule "terraform_documented_outputs" {
  enabled = true
}

# Enforce consistent naming conventions
rule "terraform_naming_convention" {
  enabled = true

  resource {
    format = "snake_case"
  }

  data {
    format = "snake_case"
  }

  variable {
    format = "snake_case"
  }

  output {
    format = "snake_case"
  }
}
