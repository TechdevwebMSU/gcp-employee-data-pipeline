#!/usr/bin/env bash
set -euo pipefail
: "${PROJECT_ID:?Set PROJECT_ID}"
: "${BUCKET:?Set BUCKET (globally unique)}"
REGION="${REGION:-us-central1}"
BQ_LOCATION="${BQ_LOCATION:-US}"
gcloud config set project "$PROJECT_ID"
gcloud services enable storage.googleapis.com bigquery.googleapis.com composer.googleapis.com \
  cloudbuild.googleapis.com dataflow.googleapis.com pubsub.googleapis.com run.googleapis.com \
  cloudfunctions.googleapis.com eventarc.googleapis.com artifactregistry.googleapis.com
if ! gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://$BUCKET" --location="$REGION"
fi
bq --location="$BQ_LOCATION" mk --dataset "$PROJECT_ID:employee_raw" || true
bq --location="$BQ_LOCATION" mk --dataset "$PROJECT_ID:employee_curated" || true
