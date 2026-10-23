from airflow.sdk import dag,task

@dag(dag_id="XCOM_DAG")
def DAG():
   @task.python
   def E_task()->dict:
      extracted_data={'data':[1,2,3]}
      return extracted_data

   @task.python
   def T_task(Data:dict)->list:
      data_list=Data['data']
      transformed_data=[2*x for x in data_list]
      return transformed_data

   @task.python
   def L_task(Data:list):
      print(Data)

   first  = E_task()
   second = T_task(first)
   third  = L_task(second)

dag_instance=DAG()
      