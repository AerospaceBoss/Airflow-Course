from airflow.sdk import dag,task
from datetime import datetime
import pendulum

update_date = pendulum.datetime(
    2026, 7, 28, tz="UTC"
)

@dag(dag_id="umberlla_braching",
     schedule="@hourly",
     catchup=False)
def DAG():
   @task
   def start():
      pass
   @task.branch 
   def branch_decider(**context):
      now=context["dag_run"].start_date

      if now < update_date:
         return  "old_branch"
      else:
         return  "new_branch"

   @task.python
   def old_branch():
      print("old_branch is taken!")

   @task.python
   def new_branch():
      print("new_branch is taken!")

   @task(trigger_rule='none_failed')
   def join():
      print("joined")

   start=start()
   A=branch_decider()
   B=old_branch()
   C=new_branch()
   D=join()

   start>>A>>[B,C]>>D

instance=DAG()