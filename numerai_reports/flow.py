import os
import datetime

import click
from prefect import task, Flow
from prefect.schedules.clocks import CronClock
from prefect.schedules import Schedule
from prefect.storage import Docker
from prefect.engine.results import GCSResult
from prefect.utilities.gcp import get_storage_client

from numerai_reports import settings
from numerai_reports import data


@task(log_stdout=False)
def fetch():
    details, leaderboard = data.fetch_from_api()
    filename_de = "details.parq"
    filename_lb = "leaderboard.parq"
    details.to_parquet(filename_de)
    leaderboard.to_parquet(filename_lb)
    return [filename_de, filename_lb]


@task(log_stdout=False, max_retries=3,
      retry_delay=datetime.timedelta(minutes=10))
def upload_to_gcs(filenames):
    storage_client = get_storage_client()
    bucket = storage_client.get_bucket(settings.CLOUD_BUCKET)
    for filename in filenames:
        blob = bucket.blob(filename)
        blob.upload_from_filename(filename)


@click.command()
@click.option('--register', is_flag=True, default=False)
@click.option('--run', is_flag=True, default=False)
def main(register, run):
    if register:
        schedule = Schedule(clocks=[CronClock("1 19 * * *")])
    else:
        schedule = None

    result = GCSResult(bucket='uuazed-prefect')
    with Flow("numerai-reports", schedule, result=result) as flow:
        filenames = fetch()
        upload_to_gcs(filenames)

    flow.storage = Docker(
        registry_url="gcr.io/numerai-171710",
        python_dependencies=['pandas', 'numerapi', 'pyarrow'],
        files={os.path.abspath("data.py"): "numerai_reports/data.py",
               os.path.abspath("settings.py"): "numerai_reports/settings.py",
               os.path.abspath("utils.py"): "numerai_reports/utils.py",
               },
        env_vars={"PYTHONPATH": "$PYTHONPATH:/"},
        secrets=["GCP_CREDENTIALS"])

    if register:
        flow.register(project_name="numerai", labels=["docker"])
    if run:
        flow.run()


if __name__ == "__main__":
    main()
