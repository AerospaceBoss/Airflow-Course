from airflow.sdk import dag, task


@dag(dag_id="conditional_DAG")
def DAG():

    @task
    def extract():
        extracted_data = {
            "fighter": "khabib",
            "record": [29, 0]
        }
        return extracted_data

    @task
    def transform_1(extracted: dict):
        return extracted["fighter"]

    @task
    def transform_2(extracted: dict):
        return extracted["record"][0]

    @task
    def transform_3(extracted: dict):
        return extracted["record"][1]

    @task.branch
    def condition(name: str, wins: int, losses: int):
        if wins > 20:
            return "prof_load"
        else:
            return "amat_load"

    @task
    def prof_load(name: str, wins: int, losses: int):
        final_data = (
            f"professional fighter name is: {name}, "
            f"number of wins is: {wins}, "
            f"number of losses: {losses}"
        )
        print(final_data)

    @task
    def amat_load(name: str, wins: int, losses: int):
        final_data = (
            f"amateur fighter name is: {name}, "
            f"number of wins is: {wins}, "
            f"number of losses: {losses}"
        )
        print(final_data)

    # Extract
    first = extract()

    # Parallel transformations
    second1 = transform_1(first)
    second2 = transform_2(first)
    second3 = transform_3(first)

    # Branch
    third = condition(second1, second2, second3)

    # Possible branches
    fourth = prof_load(second1, second2, second3)
    fifth = amat_load(second1, second2, second3)

    # Branch controls which load task runs
    third >> [fourth, fifth]


dag_instance = DAG()