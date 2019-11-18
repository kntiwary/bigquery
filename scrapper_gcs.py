# Imports the Google Cloud client library
from google.cloud import storage
import time
import datetime


# It's kind of verifying authentication

def implicit():
    """
    Setting an environment variable
    export GOOGLE_APPLICATION_CREDENTIALS="./justlikethat-af407311266b.json"

    """
    from google.cloud import storage

    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client()

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)


def get_bucket_info(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    print('ID: {}'.format(bucket.id))
    print('Name: {}'.format(bucket.name))
    print('Storage Class: {}'.format(bucket.storage_class))
    print('Location: {}'.format(bucket.location))
    print('Location Type: {}'.format(bucket.location_type))
    print('Cors: {}'.format(bucket.cors))
    print('Default Event Based Hold: {}'
          .format(bucket.default_event_based_hold))
    print('Default KMS Key Name: {}'.format(bucket.default_kms_key_name))
    print('Metageneration: {}'.format(bucket.metageneration))
    print('Retention Effective Time: {}'
          .format(bucket.retention_policy_effective_time))
    print('Retention Period: {}'.format(bucket.retention_period))
    print('Retention Policy Locked: {}'.format(bucket.retention_policy_locked))
    print('Requester Pays: {}'.format(bucket.requester_pays))
    print('Self Link: {}'.format(bucket.self_link))
    print('Time Created: {}'.format(bucket.time_created))
    print('Versioning Enabled: {}'.format(bucket.versioning_enabled))
    print('Labels: {}'.format(bucket.labels))

    # pprint.pprint(bucket.labels)


def create_bucket(bucket_name):
    """Create bucket"""
    # Instantiates a client
    storage_client = storage.Client()
    # Creates the new bucket
    bucket = storage_client.create_bucket(bucket_name)
    print('Bucket {} created.'.format(bucket.name))


# Making individual objects publicly readable
def make_public(bucket_name, blob_name):
    """Makes a blob publicly accessible."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.make_public()

    print('Blob {} is publicly accessible at {}'.format(blob.name, blob.public_url))


def upload_file(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    """
    from google.cloud import storage 
    def upload_blob(bucket_name, source_file_name, destination_blob_name, api_parameter): 
        storage_client = storage.Client() 
        bucket = storage_client.get_bucket(bucket_name) 
        blob = bucket.blob("{}/".format(api_parameter) + destination_blob_name) 
    
    """
    # NAME = "{}-conv-{}-nodes-{}-dense-{}".format(conv_layer, layer_size, dense_layer, int(time.time()))
    dir = "{}/{}/{}".format(str(datetime.datetime.now().year), str(datetime.datetime.now().strftime("%B")),
                            str(datetime.datetime.now().day))
    destination_blob_name = dir + '/' + destination_blob_name.split('.')[0] + str(int(time.time())) + '.csv'
    print(destination_blob_name, "destination_blob_name")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def download_file(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(source_blob_name, destination_file_name))


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    print("fetching urls ")
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you just specify prefix = 'a', you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a' and delimiter='/', you'll get back:

        a/1.txt

    Additionally, the same request will return blobs.prefixes populated with:

        a/b/
    """
    urls=[]
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix,
                                      delimiter=delimiter)

    print('Blobs:')
    for blob in blobs:
        urls.append(blob.public_url)
        print(blob.name,blob.public_url)

    if delimiter:
        print('Prefixes:')
        for prefix in blobs.prefixes:
            print(prefix)


    return urls

# list_blobs_with_prefix('bigquery_tarams','2019/November/18',None)
gsurls=list_blobs_with_prefix('bigquery_tarams','2019',None)
# get_bucket_info('bigquery_tarams')
# implicit()# List buckets
# upload_file('bigquery_tarams', 'mathematician2.csv', 'another3.csv')
# print(datetime.datetime.now().year)
# print(datetime.datetime.now().strftime("%B"))
# print(datetime.datetime.now().day)
# dir="{}/{}/{}".format(str(datetime.datetime.now().year),str(datetime.datetime.now().strftime("%B")),str(datetime.datetime.now().day))
# print(x)
