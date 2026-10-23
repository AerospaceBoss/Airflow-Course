from airflow.sdk import dag,task

@dag(dag_id="parallel_task_dag")
def DAG():
   @task.python
   def Extract(**kwargs):
      extracted_data={"fighter":"khabib","record":[29,0]}
      ti=kwargs["ti"]
      ti.xcom_push(key='extracted',value=extracted_data)

   @task.python
   def transform_1(**kwargs):
      ti=kwargs["ti"]
      extracted_data=ti.xcom_pull(task_ids='Extract',key='extracted')
      name=extracted_data["fighter"]
      transformed_data=name
      ti.xcom_push(key='transformed1',value=transformed_data)
   @task.python
   def transform_2(**kwargs):
         ti=kwargs["ti"]
         extracted_data=ti.xcom_pull(task_ids='Extract',key='extracted')
         wins=extracted_data["record"][0]
         transformed_data=wins
         ti.xcom_push(key='transformed2',value=transformed_data)
   @task.python
   def transform_3(**kwargs):
         ti=kwargs["ti"]
         extracted_data=ti.xcom_pull(task_ids='Extract',key='extracted')
         losses=extracted_data["record"][1]
         transformed_data=losses
         ti.xcom_push(key='transformed3',value=transformed_data)
   @task.python
   def Load(**kwargs):
       ti=kwargs["ti"]
       name=ti.xcom_pull(task_ids="transform_1",key='transformed1')
       wins=ti.xcom_pull(task_ids="transform_2",key='transformed2')
       losses=ti.xcom_pull(task_ids="transform_3",key='transformed3')
       final_data=f"fighter name is : {name} number of wins is : {wins} number of losses : {losses}"
       print(final_data)
       return final_data
   first=Extract()
   second1=transform_1()
   second2=transform_2()
   second3=transform_3()
   third=Load()
   first>>[second1,second2,second3]>>third
dag_instance=DAG()

   



      