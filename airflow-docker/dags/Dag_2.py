from airflow.sdk import dag,task

@dag(dag_id="Omar_dag")
def second_dag():
   @task.python
   def first_task():
      print("omar task1")
   @task.python
   def second_task():
      print("omar task1")
   @task.python
   def version_task():
      print("omar sameh 2.0")

   first=first_task()
   second=second_task()
   third=version_task()

   first>>second>>third
#registering the dag for being processed
second_dag()
   

   
   