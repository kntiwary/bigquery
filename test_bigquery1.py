from google.cloud import bigquery

# dataset_id = 'your-project.your_dataset'
project_id = 'justlikethat'

to_delete_dataset = []
schema = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("Name", "STRING"),
]


def create_client(project_id):
    return bigquery.Client(project=project_id)


def dataset_exists(dataset_reference, client):
    """Return if a dataset exists.
    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
            A reference to the dataset to look for.
    Returns:
        bool: ``True`` if the dataset exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False


def test_dataset_exists(client, to_delete):
    """Determine if a dataset exists."""
    DATASET_ID = "get_table_dataset_{}".format(_millis())
    dataset_ref = client.dataset(DATASET_ID)
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)
    to_delete.append(dataset)

    assert dataset_exists(client, dataset_ref)
    assert not dataset_exists(client, client.dataset("dataset doesnot exist"))


def list_dataset(client):
    datasets = list(client.list_datasets())
    project = client.project

    if datasets:
        print("Datasets in project {}:".format(project))
        for dataset in datasets:  # API request(s)
            print("\t{}".format(dataset.dataset_id))
    else:
        print("{} project does not contain any datasets.".format(project))


def create_dataset(dataset_id, client):
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.description = 'my dataset'
    dataset = client.create_dataset(dataset)  # API request
    print("dataset created")
    to_delete_dataset.append(dataset)
    return dataset


def get_dateset_info(project_id, dataset_id, client):
    dataset_id = "{}.{}".format(project_id, dataset_id)

    dataset = client.get_dataset(dataset_id)
    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    friendly_name = dataset.friendly_name
    print(
        "Got dataset '{}' with friendly_name '{}'.".format(
            full_dataset_id, friendly_name
        )
    )

    # View dataset properties
    print("Description: {}".format(dataset.description))
    print("Labels:")
    labels = dataset.labels
    if labels:
        for label, value in labels.items():
            print("\t{}: {}".format(label, value))
    else:
        print("\tDataset has no labels defined.")

    # View tables in dataset
    print("Tables:")
    tables = list(client.list_tables(dataset))  # API request(s)
    if tables:
        for table in tables:
            print("\t{}".format(table.table_id))
    else:
        print("\tThis dataset does not contain any tables.")


def create_table(project_id, dataset_id, table_name, schema, client):
    """

    :param project_id: name of the project
    :param dataset_id: name of the dataset
    :param table_name: name of the table to be created
    :param schema: schema must be list
    :param client:client object
    :return: none
    """
    table_id = "{}.{}.{}".format(project_id, dataset_id, table_name)
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # API request
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def list_table(project_id, dataset_id, client):
    dataset_id = '{}.{}'.format(project_id, dataset_id)

    tables = client.list_tables(dataset_id)

    print("Tables contained in '{}':".format(dataset_id))
    for table in tables:
        print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))


def run_query(query, client):
    QUERY = query
    # query_job = client.query(QUERY)
    # for row in query_job:  # API request
    #     # Row values can be accessed by field name or index
    #     # print(row.Name)
    #     assert row[1] == row.Name == row['Name']

    TIMEOUT = 30  # in seconds
    query_job = client.query(QUERY)  # API request - starts the query
    assert query_job.state == 'RUNNING'

    # Waits for the query to finish
    iterator = query_job.result(timeout=TIMEOUT)
    rows = list(iterator)

    assert query_job.state == 'DONE'
    # assert len(rows) == 100
    row = rows[1]
    print(row)
    # pass


def delete_dataset(dataset_id, client):
    client.delete_dataset(dataset_id)  # API request
    print('dataset {} deleted successfully'.format(dataset_id))


def refresh(dataset, client):
    """
    Refresh metadata for a dataset (to pick up changes made by another client)
    """
    pass


def delete_test_datasets(client):
    for d in to_delete_dataset:
        delete_dataset(d, client)


client = create_client('justlikethat')
list_dataset(client)
# test_query = 'SELECT * FROM `justlikethat.bucket_upload.BabyNames` LIMIT 100'

# run_query(test_query, client)
# get_dateset_info('justlikethat','bucket_upload', client)
# d_set = create_dataset('test', client)
# create_table('justlikethat', 'bucket_upload', 'test_table', schema, client)
# delete_test_datasets(client)
# delete_dataset('justlikethat.'+'test',client)##
