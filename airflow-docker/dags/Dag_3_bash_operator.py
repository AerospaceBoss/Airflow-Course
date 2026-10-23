from airflow.sdk import dag,task
from airflow.providers.standard.operators.bash import BashOperator

@dag(dag_id="Bash_dag")
def DAG():
   @task.bash
   def bash_task()->str:
    return "echo 'Hello World'"

   bash_old_school = BashOperator(
    task_id="bash_old_school",
    bash_command="echo 'Hello from old school BashOperator'"
   )
   first=bash_task()
   second=bash_old_school

   first>>second
DAG()