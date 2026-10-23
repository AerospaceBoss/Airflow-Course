from airflow.sdk import dag, task
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from datetime import datetime


@dag(
    dag_id="conditional_task_skip_built_in",
    schedule="@daily",
    start_date=datetime(2026, 8, 26),
    catchup=False
)
def DAG():

    @task
    def train_model():
        print("model is trained!")

    @task
    def deploy():
        print("DAG is deployed!")

    latest_only = LatestOnlyOperator(
        task_id="decider"
    )

    first = train_model()
    second = deploy()

    first >> latest_only >> second


instance = DAG()