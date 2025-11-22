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

resource "google_bigquery_dataset" "default" {
    dataset_id                  = "sportsETL"
    description                 = "Contains tables relevant to the sportsETL project"
    }

resource "google_bigquery_table" "default" {
    dataset_id = google_bigquery_dataset.default.dataset_id
    table_id   = "wizards_game_stats"
    deletion_protection = false

    schema = <<EOF
    [
    {
        "name": "home_or_away",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "game_id",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "outcome",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "field_goals_made",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "field_goals_attempts",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "field_goals_pct",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "threes_made",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "threes_attempts",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "threes_pct",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "free_throws_made",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "free_throw_attempts",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "free_throw_pct",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "rebounds_total",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "rebounds_off",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "rebounds_def",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "assists_total",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "steals_total",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "blocks_total",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "personal_fouls_total",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "turnovers_total",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "team",
        "type": "STRING",
        "mode": "REQUIRED"
    }
    ]
    EOF

}

resource "random_string" "example" {
  length  = 10
  lower   = true
  upper   = false
  numeric = true
  special = false
}   