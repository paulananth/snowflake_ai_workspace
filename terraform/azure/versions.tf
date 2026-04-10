terraform {
  required_version = ">= 1.7.0"

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }

    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.5"
    }

    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.67"
    }

    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }

    time = {
      source  = "hashicorp/time"
      version = "~> 0.13"
    }
  }
}

provider "azurerm" {
  features {}

  subscription_id = var.subscription_id != "" ? var.subscription_id : null
}

provider "azapi" {}
