from airflow.sdk import dag,task

@dag(dag_id="incremental_load_DAG")
def DAG():
   @task.python
   def Extract(**kwargs):
      interval_start=kwargs['data_interval_start']
      interval_end=kwargs['data_interval_end']
      trial=kwargs['logical_date']
      #select * from fact where date between interval_start and interval_end
      extracted_data={"fighter":"khabib","record":[29,0]}
      return extracted_data

   @task.python
   def transform_1(extracted:dict):
      name=extracted["fighter"]
      return name
       
   @task.python
   def transform_2(extracted:dict):
         wins=extracted["record"][0]
         return wins
   @task.python
   def transform_3(extracted:dict):
         losses=extracted["record"][1]
         return losses
   @task.python
   def Load(name:str,wins:int,losses:int):
       final_data=f"fighter name is : {name} number of wins is : {wins} number of losses : {losses}"
       print(final_data)
       
   first=Extract()
   second1=transform_1(first)
   second2=transform_2(first)
   second3=transform_3(first)
   third=Load(second1,second2,second3)
   first>>[second1,second2,second3]>>third
dag_instance=DAG()

   



      