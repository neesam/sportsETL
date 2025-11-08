terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.10.0"
    }
  }
}

provider "google" {
  project     = var.project_name
  region      = var.region
  credentials = var.credentials
}

resource "google_storage_bucket" "sportsPipeline" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
}

resource "google_service_account" "dbt" {
  account_id   = var.dbt_service_account_name
  display_name = "A service account that dbt uses"
}

resource "google_project_iam_member" "dbt-account-iam" {
  project = var.project_name
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${var.dbt_service_account_name}@${var.project_name}.iam.gserviceaccount.com"
}

resource "google_service_account" "airflow" {
  account_id   = var.airflow_service_account_name
  display_name = "A service account that airflow uses"
}

resource "google_project_iam_member" "airflow-account-iam" {
  project = var.project_name
  role    = "roles/storage.admin"
  member  = "serviceAccount:${var.airflow_service_account_name}@${var.project_name}.iam.gserviceaccount.com"
}
resource "random_string" "example" {
  length  = 10
  lower   = true
  upper   = false
  numeric = true
  special = false
}   