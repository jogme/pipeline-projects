import pendulum

from airflow.decorators import dag, task


@dag()
def bkk_trip()
    """
    ### Pipeline for BKK trip update
    This loads the API of https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb
    and loads it in a MongoDB database
    """
    @task()
    def extract():
        pass

    @task()
    def transform(input):
        pass

    @task()
    def load(processed_input):
        pass

    data = extract()
    processed_data = transform(data)
    load(processed_data)

bkk_trip()
