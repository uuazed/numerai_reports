import os
import datetime

import click
from prefect import task, Flow
from prefect.schedules.clocks import CronClock
from prefect.schedules import Schedule
from prefect.storage import Docker

from google.cloud import storage

import settings
import data


@task(log_stdout=False, max_retries=3,
      retry_delay=datetime.timedelta(minutes=10))
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
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(settings.CLOUD_BUCKET)
    for filename in filenames:
        blob = bucket.blob(filename)
        blob.upload_from_filename(filename)


@click.command()
@click.option('--register', is_flag=True, default=False)
@click.option('--run', is_flag=True, default=False)
def main(register, run):
    if register:
        schedule = Schedule(clocks=[CronClock("1 20 * * *")])
    else:
        schedule = None

    with Flow("numerai-reports", schedule) as flow:
        filenames = fetch()
        upload_to_gcs(filenames)

    flow.storage = Docker(
        registry_url="gcr.io/numerai-171710",
        python_dependencies=['pandas', 'numerapi', 'pyarrow'],
        files={os.path.abspath("data.py"): "modules/data.py",
               os.path.abspath("settings.py"): "modules/settings.py",
               os.path.abspath("utils.py"): "modules/utils.py",
               },
        env_vars={"PYTHONPATH": "$PYTHONPATH:modules/"},)

    if register:
        flow.register(project_name="numerai", labels=["docker"])
    if run:
        flow.run()


if __name__ == "__main__":
    main()
