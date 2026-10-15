#!/usr/bin/env bash
set -euo pipefail
: "${BUCKET:?Set BUCKET}"
gcloud storage cp data/employees_part_*.csv "gs://$BUCKET/incoming/employees/"
