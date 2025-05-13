from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials/kitrum-bq.json'
client = bigquery.Client()


def get_data_from_bq(sql_query):
    query_job = client.query(sql_query)
    bq_data = []
    for row in query_job.result():
        row_dict = {}
        for key in row.keys():
            row_dict[key] = row[key]
        bq_data.append(row_dict)
    return bq_data


def run_query(sql_query):
    query_job = client.query(sql_query)
    query_job.result()


def insert_to_bigquery(data_to_insert, table_id):
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(data_to_insert, table_id, job_config=job_config)
    job.result()

