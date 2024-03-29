

def sample_create_transfer_config(project_id, dataset_id, authorization_code="4/tQHKhZK2HDDNHvRXiIlIZETGdDnrDNH8L8UXLSZFPY6iHH5J7l-lAQa2H9LaXVigT2ebSethn_hSDHzbrUYLqkg"):
    # [START bigquerydatatransfer_create_scheduled_query]
    from google.cloud import bigquery_datatransfer
    import google.protobuf.json_format

    client = bigquery_datatransfer.DataTransferServiceClient()

    # TODO(developer): Set the project_id to the project that contains the
    #                  destination dataset.
    # project_id = "your-project-id"

    # TODO(developer): Set the destination dataset. The authorized user must
    #                  have owner permissions on the dataset.
    # dataset_id = "your_dataset_id"

    # TODO(developer): The first time you run this sample, set the
    # authorization code to a value from the URL:
    # https://www.gstatic.com/bigquerydatatransfer/oauthz/auth?client_id=433065040935-hav5fqnc9p9cht3rqneus9115ias2kn1.apps.googleusercontent.com&scope=https://www.googleapis.com/auth/bigquery%20https://www.googleapis.com/auth/drive&redirect_uri=urn:ietf:wg:oauth:2.0:oob
    #
    # authorization_code = "_4/ABCD-EFGHIJKLMNOP-QRSTUVWXYZ"
    #
    # You can use an empty string for authorization_code in subsequent runs of
    # this code sample with the same credentials.
    #
    # authorization_code = ""

    # Use standard SQL syntax for the query.
    query_string = """
    SELECT
      CURRENT_TIMESTAMP() as current_time,
      @run_time as intended_run_time,
      @run_date as intended_run_date,
      17 as some_integer
    """

    parent = client.project_path(project_id)
    transfer_config = google.protobuf.json_format.ParseDict(
        {
            "destination_dataset_id": dataset_id,
            "display_name": "Your Scheduled Query Name",
            "data_source_id": "scheduled_query",
            "params": {
                "query": query_string,
                # "destination_table_name_template": "your_table_{run_date}",
                "destination_table_name_template":'BabyNames',
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            "schedule": "every 24 hours",
        },
        bigquery_datatransfer.types.TransferConfig(),
    )

    response = client.create_transfer_config(
        parent, transfer_config, authorization_code=authorization_code
    )

    print("Created scheduled query '{}'".format(response.name))
    # [END bigquerydatatransfer_create_scheduled_query]
    # Return the config name for testing purposes, so that it can be deleted.
    return response.name


# def main():
#     import argparse
#
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--project_id", type=str, default="your-project-id")
#     parser.add_argument("--dataset_id", type=str, default="your_dataset_id")
#     parser.add_argument("--authorization_code", type=str, default="")
#     args = parser.parse_args()
#
#     sample_create_transfer_config(args.project_id, args.authorization_code)
#
#
# if __name__ == "__main__":
#     main()
auth_code='_4/tQHKhZK2HDDNHvRXiIlIZETGdDnrDNH8L8UXLSZFPY6iHH5J7l-lAQa2H9LaXVigT2ebSethn_hSDHzbrUYLqkg'
project_id='justlikethat'
dataset_id='bucket_upload'
sample_create_transfer_config(project_id,dataset_id,auth_code)