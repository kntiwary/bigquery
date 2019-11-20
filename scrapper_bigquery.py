from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from scrapper_gcs import list_blobs_with_prefix

"""
Documentation
https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html
"""


# bucket uri = 'gs://bigquery_tarams'
# dataset_id =  'justlikethat:bucket_upload'
# table = 'BabyNames'

def create_client(project_id):
    return bigquery.Client(project=project_id)


def load_data(dataset_id, table_name, gsuri):
    # Initialize a BigQuery Client
    client = bigquery.Client()
    uri = gsuri
    table = table_name
    """
    Loading CSV data into a table
    """

    dataset = dataset_id
    dataset_ref = client.dataset(dataset)

    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("Name", "STRING"),
    ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV  # The source format defaults to CSV, so the line is optional.

    load_job = client.load_table_from_uri(uri, dataset_ref.table(table), job_config=job_config)  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table))
    print("Loaded {} rows.".format(destination_table.num_rows))


def load_data_list(dataset_id, table_name, gsuri=[]):
    # Initialize a BigQuery Client
    client = bigquery.Client()
    uris = gsuri
    table = table_name
    """
    Loading CSV data into a table
    """

    dataset = dataset_id
    dataset_ref = client.dataset(dataset)

    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("Name", "STRING"),
    ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV  # The source format defaults to CSV, so the line is optional.
    for uri in uris:
        load_job = client.load_table_from_uri(uri, dataset_ref.table(table), job_config=job_config)  # API request
        print("Starting job {} for file ".format(load_job.job_id, uri))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.file {} uploaded.".format(uri))

        destination_table = client.get_table(dataset_ref.table(table))
        print("Loaded {} rows.".format(destination_table.num_rows))


def write_empty(dataset_id, table_name, gsuri):
    """
    Write if empty
    Writes the data only if the table is empty.
    """

    client = bigquery.Client()
    # table_ref = client.dataset('my_dataset').table('existing_table')
    table_ref = client.dataset(dataset_id).table(table_name)
    uri = gsuri

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    # uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))
    try:
        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(table_ref)
        print("Loaded {} rows.".format(destination_table.num_rows))
    except Conflict as e:
        print("Exception {}".format(e))


def write_append(dataset_id, table_name, gsuri):
    """
    Append to table
    Appends the data to the end of the table.
    """

    client = bigquery.Client()
    # table_ref = client.dataset('my_dataset').table('existing_table')
    table_ref = client.dataset(dataset_id).table(table_name)
    uri = gsuri

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    # uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(table_ref)
    print("Loaded {} rows.".format(destination_table.num_rows))


def write_truncate(dataset_id, table_name, gsuri):
    """
    Overwrite table
    Erases all existing data in a table before writing the new data.
    """
    client = bigquery.Client()
    # table_ref = client.dataset('my_dataset').table('existing_table')
    table_ref = client.dataset(dataset_id).table(table_name)
    uri = gsuri

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    # uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(table_ref)
    print("Loaded {} rows.".format(destination_table.num_rows))


# load_data('bucket_upload', 'BabyNames', 'gs://bigquery_tarams/mathematician.csv')

client = create_client('justlikethat')

# gsurls = list_blobs_with_prefix('bigquery_tarams', '2019', None)
gsurls = list_blobs_with_prefix('tarams_new', '2019', None)

print(gsurls)

# load_data('bucket_upload', 'BabyNames', 'gs://bigquery_tarams/mathematician.csv')
load_data_list('bucket_upload', 'BabyNames', gsurls)
# write_truncate('bucket_upload', 'BabyNames',
#                'gs://bigquery_tarams/test.csv')  # Erases all existing data in a table before writing the new data.
# write_append('bucket_upload', 'BabyNames',
#              'gs://bigquery_tarams/another2.csv')  # Appends the data to the end of the table.
# write_empty('bucket_upload', 'BabyNames',
#             'gs://bigquery_tarams/mathematician.csv')  # Writes the data only if the table is empty.
