from airflow.sdk import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(dag_id="parent_dag")
def parent_dag():

    first_trig = TriggerDagRunOperator(
        task_id="first_dag_trig",
        trigger_dag_id="first_dag"
    )

    second_trig = TriggerDagRunOperator(
        task_id="second_dag_trig",
        trigger_dag_id="parallel_task_dag"
    )

    first_trig >> second_trig
parent_dag()