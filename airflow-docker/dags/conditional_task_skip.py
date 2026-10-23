from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.timetables.base import DataInterval, TimeRestriction

from datetime import datetime

import pendulum


@dag(
    dag_id="conditional_task_skip",
    schedule="@daily",
    start_date=datetime(2026, 8, 26),
    catchup=True
)
def DAG():

    @task
    def train_model():
        print("model is trained!")

    @task
    def deploy():
        print("DAG is deployed!")

    @task
    def deploy_decider(**context):

        # Get the current DAG run's data interval
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]

        # Get the DAG object
        dag = context["dag"]

        # Create a DataInterval representing the current run
        current_interval = DataInterval(
            start=data_interval_start,
            end=data_interval_end
        )

        # Define timetable restrictions
        time_restriction = TimeRestriction(
            earliest=None,
            latest=None,
            catchup=True
        )

        # Ask Airflow for information about the next DAG run
        next_info = dag.timetable.next_dagrun_info(
            last_automated_data_interval=current_interval,
            restriction=time_restriction
        )

        # If there is no next DAG run,
        # this is the latest run.
        if next_info is None:
            print("Latest DAG run -> deploy")
            return

        # Otherwise, this is an older/backfill run.
        raise AirflowSkipException(
            "Backfill DAG run -> skipped!"
        )

    first = train_model()

    decide = deploy_decider()

    second = deploy()

    first >> decide >> second


instance = DAG()