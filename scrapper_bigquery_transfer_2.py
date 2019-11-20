import google
from google.cloud import bigquery_datatransfer, bigquery_datatransfer_v1
import google.protobuf.json_format

project = 'justlikethat'  # TODO: Update to your project ID.
datasource = 'google_cloud_storage'
dataset = 'bucket_upload'
table = 'BabyNames'
auth_code = '_4/tQHKhZK2HDDNHvRXiIlIZETGdDnrDNH8L8UXLSZFPY6iHH5J7l-lAQa2H9LaXVigT2ebSethn_hSDHzbrUYLqkg'
query_string = """
   SELECT
     CURRENT_TIMESTAMP() as current_time,
     @run_time as '2020-02-15 00:00:00'
   """

PARAMS = {
    "query": query_string,
    "data_path_template": "gs://tarams_new/2019/November/19/another31574142001.csv",
    "destination_table_name_template": table,
    "file_format": "CSV",
    # "write_disposition": "WRITE_TRUNCATE",
    # "max_bad_records":"1",
    # "ignore_unknown_values":"true",
    #
    # "skip_leading_rows":"1",
    # "allow_quoted_newlines":"true",
    # "allow_jagged_rows":"false",
    # "delete_source_files":"true"
}

# schedule_options = {"disable_auto_scheduling": True}
# transfer_config = {
#     'destination_dataset_id': dataset,
#     'display_name': 'test',
#     'data_source_id': datasource,
#     # 'params':PARAMS,
#
#     'schedule': 'every 24 hours'
# }
# transfer_config={
#     "destination_dataset_id": dataset,
#     "display_name": "test",
#     "data_source_id": 'google_cloud_storage',
#     "params": {
#         "data_path_template":"gs://tarams_new/2019/November/19/another31574142001.csv",
#         "destination_table_name_template":table,
#         "file_format":"CSV",
#         "max_bad_records":"1",
#         "ignore_unknown_values":"true",
#
#         "skip_leading_rows":"1",
#         "allow_quoted_newlines":"true",
#         "allow_jagged_rows":"false",
#         "delete_source_files":"true"
#     },
#     "schedule": "every 24 hours",
# }
transfer_config = google.protobuf.json_format.ParseDict({
    "name": 'test',
    "destination_dataset_id": dataset,
    "display_name": 'test1',
    "data_source_id": datasource,
    "params": PARAMS,
    "schedule": "3rd monday of month 15:30",
},bigquery_datatransfer_v1.types.TransferConfig()
)
# transfer_config = {
#     "name": 'test',
#     "destination_dataset_id": dataset,
#     "display_name": 'test1',
#     "data_source_id": datasource,
#     "params": PARAMS,
#     "schedule": "3rd monday of month 15:30",
#
# }
#

# transfer_config = google.protobuf.json_format.ParseDict(
#     {
#         "destination_dataset_id": dataset,
#         "display_name": "test1",
#         "data_source_id": datasource,
#         "params": {
#             "query": query_string,
#             "destination_table_name_template": table,
#             "write_disposition": "WRITE_TRUNCATE",
#             "partitioning_field": "",
#         },
#         "schedule": "every 24 hours",
#     },
#     bigquery_datatransfer_v1.types.TransferConfig(),
# )


def create_client():
    return bigquery_datatransfer.DataTransferServiceClient()
    # return bigquery_datatransfer_v1.DataTransferServiceClient()


def get_parent(project, client):
    return client.project_path(project)


def get_name(project, datasource, client):
    return client.project_data_source_path(project, datasource)


def validate_name(name, client):
    response = client.check_valid_creds(name)
    print(response)


def create_transfer_config(parent, transfer_config, authorization_code, client):
    print(transfer_config)
    response = client.create_transfer_config(parent, transfer_config, authorization_code)
    print(response.name)


client = create_client()
parent = get_parent(project, client)
name = get_name(project, datasource, client)
print("Parent-", parent)
print("Name-", name)
validate_name(name, client)
create_transfer_config(parent, transfer_config, auth_code, client)

# if validate_name(name, client):
#     pass
#
# else:
#     print("Invalid name")
