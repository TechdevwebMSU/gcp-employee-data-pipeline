
import json
import random
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

PROJECT_ID = "project-67cb1491-36cb-437f-b95"
TOPIC = f"projects/{PROJECT_ID}/topics/employee-events"

FIRST_NAMES = ["Rahul", "Priya", "Amit", "Sarah", "Neha"]
LAST_NAMES = ["Sharma", "Patel", "Joshi", "Smith", "Kumar"]
DEPARTMENTS = ["ENGINEERING", "FINANCE", "HR", "SALES"]


class GenerateEmployee(beam.DoFn):
    def process(self, timestamp):
        now = datetime.now(timezone.utc)

        employee_id = int(timestamp * 1000)
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)

        employee = {
            "employee_id": employee_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}.{last_name.lower()}.{employee_id}@example.com",
            "department": random.choice(DEPARTMENTS),
            "salary": random.randint(50000, 150000),
            "hire_date": now.date().isoformat(),
            "updated_at": now.isoformat(),
        }

        yield json.dumps(employee).encode("utf-8")


def run():
    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region="us-central1",
        temp_location="gs://employee-pipeline-neel-2026/dataflow/temp",
        staging_location="gs://employee-pipeline-neel-2026/dataflow/staging",
        job_name="employee-stream-generator",
        service_account_email="dataflow-practice-sa@project-67cb1491-36cb-437f-b95.iam.gserviceaccount.com",
        save_main_session=True,
    )

    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Generate Every 5 Seconds" >> PeriodicImpulse(
                fire_interval=5,
                apply_windowing=False,
            )
            | "Create Employee Records" >> beam.ParDo(GenerateEmployee())
            | "Publish to PubSub" >> WriteToPubSub(topic=TOPIC)
        )


if __name__ == "__main__":
    run()
