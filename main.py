from weather_collector import getRawData
from helper import TransformationLoad
from constant import API_KEY, STORAGE_BUCKET_NAME, CITIES_FILE
from bq_operations import load_csv_to_bigquery
from datetime import datetime, timedelta

latest_file_name = ""

def run_etl_process():
    global latest_file_name

    ext = getRawData(CITIES_FILE, API_KEY, STORAGE_BUCKET_NAME)
    ext.run_ext()

    trnsctnload = TransformationLoad(CITIES_FILE, API_KEY, STORAGE_BUCKET_NAME)
    latest_file_name = trnsctnload.run_etl()
    print(f"ETL process completed. Latest file: {latest_file_name}")

def load_data_to_bigquery():
    if not latest_file_name:
        print("No file to load. Please run ETL process first.")
        return

    bucket_name = "tk_project_24"
    dataset_id = "dataflow_dev"
    table_id = "tk_project_24"
    project_id = "oredata-de-cirrus"
    load_csv_to_bigquery(bucket_name, latest_file_name, dataset_id, table_id, project_id)
    print(f"Data loaded to BigQuery from file: {latest_file_name}")

if __name__ == '__main__':
    run_etl_process()
    load_data_to_bigquery()
