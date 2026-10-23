from airflow.sdk import dag , task
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
cairo = ZoneInfo("Africa/Cairo")
@dag(
    dag_id="Scheduled_dag",
    start_date=datetime(2026, 8, 21, tzinfo=cairo),
    schedule=timedelta(minutes=30),
    end_date=datetime(2026, 8, 22, tzinfo=cairo),
    is_paused_upon_creation=True,
    catchup=False,
)
def first_dag():
   @task.python
   def first_task():
      print("first_task")

   @task.python 
   def second_task():
      print("second_task")

   @task.python 
   def third_task():
      print("third_task-another edit to check")
   # Dependency define
   first=first_task()
   second=second_task()
   third=third_task()

   first >> second >> third

#registering the dag
first_dag() 