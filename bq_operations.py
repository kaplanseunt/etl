from google.cloud import bigquery
from google.cloud import storage

def load_csv_to_bigquery(bucket_name, latest_file_name, dataset_id, table_id, project_id):
    bigquery_client = bigquery.Client(project=project_id)

    # GCS dosya yolu
    uri = f"gs://{bucket_name}/{latest_file_name}"
    
    # BigQuery  yukleme
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Eğer başlık satırı varsa, bunu atla
        autodetect=True,  # Otomatik olarak şema algılama
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Mevcut tabloyu yeniden oluştur
    )

    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    load_job = bigquery_client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )

    print(f"Starting job {load_job.job_id}")

    load_job.result()

    destination_table = bigquery_client.get_table(table_ref)
    print(f"Loaded {destination_table.num_rows} rows into {table_id}.")


