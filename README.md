# GCP Employee Data Pipeline (Portfolio Project)

## Phase 1
Three synthetic employee CSVs → GCS → Cloud Composer (Airflow) → BigQuery raw → BigQuery curated. The demo DAG runs every minute **only for testing**. It uses `WRITE_TRUNCATE` for the raw snapshot and `CREATE OR REPLACE TABLE` for curated output, making repeated runs idempotent for this small sample. It is **not** a production incremental ingestion design.

### Prerequisites
- GCP project with billing enabled and permissions to enable APIs, create buckets/datasets, and administer Composer
- gcloud CLI, bq CLI, Python 3, Git, VS Code
- Composer environment with the Google provider installed; choose a supported Composer 3 environment

### Start
```bash
export PROJECT_ID=your-gcp-project
export BUCKET=your-globally-unique-bucket-name
export REGION=us-central1
export BQ_LOCATION=US
bash scripts/setup.sh
bash scripts/upload.sh
```

### Create Composer
In Google Cloud Console → Cloud Composer → Create environment. Select Composer 3, `us-central1`, a small supported environment configuration, and an environment service account with the required GCS and BigQuery permissions. Composer is billable while running. Do not store service account keys in Git.

In the Airflow UI, set Variables:
- `gcp_project_id` = your GCP project ID
- `employee_bucket` = your bucket name
- `bq_location` = `US`

Find the Composer **DAGs bucket path** in the Composer environment details. Upload:
```bash
gcloud storage cp dags/employee_pipeline.py gs://YOUR_COMPOSER_BUCKET/dags/
```
Do not confuse the Composer DAGs bucket with the employee input bucket.

To push the source project to GitHub, create an empty private/public repo, then:
```bash
git init
git add .
git commit -m "Initial GCP employee pipeline"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/gcp-employee-pipeline.git
git push -u origin main
```

GitHub does **not** automatically deploy DAGs to Composer. The `gcloud storage cp` command above deploys the DAG file; a CI/CD GitHub Actions workflow can automate it later.

### Test and validate
- Wait for the DAG to appear in Airflow. Unpause it and trigger once manually, or wait for the next minute boundary while unpaused.
- The cron expression `* * * * *` means **every minute**, not a single run next minute. Pause the DAG after the test to avoid repeated executions.
- Check task logs and query:
```sql
SELECT COUNT(*) AS raw_rows FROM `PROJECT_ID.employee_raw.employees`;
SELECT COUNT(*) AS curated_rows FROM `PROJECT_ID.employee_curated.employees`;
SELECT department, COUNT(*) AS employees FROM `PROJECT_ID.employee_curated.employees` GROUP BY department;
```
Expected for the included files: 30 raw and 30 curated rows.

### Phase 2 design (next implementation)
External REST API → bounded Apache Beam Dataflow job → Pub/Sub topic → Pub/Sub-triggered Cloud Run function → GCS `incoming/employees/` CSV files → existing Airflow DAG. Keep the Pub/Sub-to-GCS function from overwriting unrelated files; use unique object names. API ingestion requires selecting an API and its authentication/rate-limit model. The existing snapshot DAG will reread all files, so API output must match the same CSV schema. For production, prefer event-triggered DAG execution and incremental MERGE.

### Cleanup
Pause the DAG and delete the Composer environment when done; delete test Dataflow jobs and other resources when no longer needed. Review billing before creating Composer.
