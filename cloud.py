import oauth2client
import uuid
import time
from gcloud import bigquery as bq
#from google.cloud import bigquery as bq
from oauth2client.client import GoogleCredentials

# Configuration
BILLING_PROJECT_ID = 'onyx-cumulus-196507'
DATASET_NAME = 'bigquery123'
TABLE_NAME = 'airport'
BUCKET_NAME = 'satish123'
FILE = 'airport.csv'
SOURCE = 'https://storage.cloud.google.com/satish123/airport.csv?_ga=2.200274028.-331489596.1519587350&_gac=1.252996475.1519744301.CjwKCAiAoNTUBRBUEiwAWje2ltt6Onlm-oURmJ0zEqOD_dy_wmi_5yUsCdGXFro37ANM_5QjwIFk5RoC4PUQAvD_BwE'.format(BUCKET_NAME, FILE)

SCHEMA = [
    bq.SchemaField('name', 'STRING', mode='required'),
    bq.SchemaField('country', 'STRING', mode='required'),
    bq.SchemaField('area_code', 'STRING', mode='required'),
    bq.SchemaField('origin', 'STRING', mode='required')
]

# CREDENTIALS = GoogleCredentials.get_application_efault()

client = bq.Client(project=BILLING_PROJECT_ID)


# Dataset
# Check if the dataset exists
def create_datasets(name):
    dataset = client.dataset(name)
    try:
        assert not dataset.exists()
        dataset.create()
        assert dataset.exists()
        print("Dataset {} created".format(name))
    except(AssertionError):
        pass


def load_data_from_gcs(dataset_name, table_name, source, schema):
    '''
    Load Data from Google Cloud Storage
    '''
    dataset = client.dataset(dataset_name)
    table = dataset.table(table_name)
    table.schema = schema
    table.create()
    job_name = str(uuid.uuid4())
    job = client.load_table_from_storage(
        job_name, table, source)
    job.source_format = 'NEWLINE_DELIMITED_JSON'

    job.begin()
    wait_for_job(job)

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


load_data_from_gcs(dataset_name=DATASET_NAME,
                   table_name=TABLE_NAME,
                   source=SOURCE,
                   schema=SCHEMA)