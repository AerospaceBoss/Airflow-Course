from airflow.sdk import dag,task

@dag(dag_id="first_dag")
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