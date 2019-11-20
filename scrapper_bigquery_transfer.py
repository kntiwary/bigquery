"""
package
pip install google-cloud-bigquery-datatransfer
"""


from google.cloud import bigquery_datatransfer

client = bigquery_datatransfer.DataTransferServiceClient()

project = 'justlikethat'  # TODO: Update to your project ID.
datasource='google_cloud_storage'

# Get the full path to your project.
parent = client.project_path(project)
print(parent)

name = client.project_data_source_path(project, datasource)
# print('Supported Data Sources:')
response = client.check_valid_creds(name)
print(response)

# Iterate over all possible data sources.
availbale_datasource=[]
# for data_source in client.list_data_sources(parent):
#     availbale_datasource.append(data_source)
    # print('{}:'.format(data_source.display_name))
    # print('\tID: {}'.format(data_source.data_source_id))
    # print('\tFull path: {}'.format(data_source.name))
    # print('\tDescription: {}'.format(data_source.description))
    # print(100*'-')

    # print('{}'.format(data_source.display_name))
print(availbale_datasource)

# for page in client.list_data_sources(parent).pages:
#     for element in page:
#         print(element)

