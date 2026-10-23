from airflow.sdk import asset
import os

@asset(
    schedule="@daily",
    uri="/opt/airflow/logs/data/data_extract.txt",
    name="first_asset"
)
def task1(context):
    uri = context['uri']
    ti = context["ti"]

    os.makedirs(os.path.dirname(uri), exist_ok=True)

    with open(uri, "w") as f:
        f.write("Data fetched successfully")

    print(f"Data written successfully to {uri}")

    ti.xcom_push(
        key="data_status",
        value="Done successfully"
    )


@asset(schedule=task1,name='asset2')
def task2(context):
    ti = context["ti"]

    status = ti.xcom_pull(
        task_ids="task_1",
        key="data_status"
    )

    return status