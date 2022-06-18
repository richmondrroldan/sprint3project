from datetime import timedelta
import boto3
import os
from io import StringIO
import feedparser
import pandas as pd
from os import listdir
from os.path import isfile, join

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
#from pathy import Bucket
# from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
# from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from datetime import datetime

# Our bucket
BUCKET_NAME = "news_sites"
COMBINED = "rich/combined/"
RAW_EXTRACT_RICH = "rich_inquirer/"
RAW_EXTRACT_GERARD = 'gerard_philstar/'

def get_files_inquirer(service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    prefix = "rich_inquirer/"

    result = gcs_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
    inquirer_files = [f for f in result]
    return result

remote_files_inquirer = get_files_inquirer()

def get_files_philstar(service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    prefix = "gerard_philstar/"

    result = gcs_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
    inquirer_files = [f for f in result]
    return result

remote_files_philstar = get_files_philstar()

def download_file_from_gcs(remote_files, local_path):
    path_files = remote_files['Contents']

    lst = []
    for idx, x in enumerate(path_files):
        lst.append(x['Key'])
        
    file_list = lst[1:]
    file_names = pd.DataFrame(file_list)
    file_names[['Folder', 'File']] = file_names[0].str.split('/', expand=True)
    
    gcs_client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    for idx, rows in file_names.iterrows():
        gcs_client.download_file(Bucket=BUCKET_NAME, Key=rows[0], Filename=local_path + rows['File'])


def upload_formatted_rss_feed_rich(feed, feed_name):
    feed = feedparser.parse(feed)
    df = pd.DataFrame(feed['entries'])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = feed_name + '_' + time_now + '.csv'
    upload_string_to_gcs_raw_rich(csv_body=csv_buffer, uploaded_filename=filename)


def upload_string_to_gcs_raw_rich(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    gcs_resource.Object(BUCKET_NAME, RAW_EXTRACT_RICH + uploaded_filename).put(Body=csv_body.getvalue())

def upload_formatted_rss_feed_gerard(feed, feed_name):
    feed = feedparser.parse(feed)
    df = pd.DataFrame(feed['entries'])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = feed_name + '_' + time_now + '.csv'
    upload_string_to_gcs_raw_gerard(csv_body=csv_buffer, uploaded_filename=filename)


def upload_string_to_gcs_raw_gerard(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    gcs_resource.Object(BUCKET_NAME, RAW_EXTRACT_GERARD + uploaded_filename).put(Body=csv_body.getvalue())


def upload_combined_to_gcs_raw(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    gcs_resource.Object(BUCKET_NAME, COMBINED + uploaded_filename).put(Body=csv_body.getvalue())


def upload_unique_to_gcs_from_raw_inquirer(feed_name):
    file_path ='/data/inquirer/'
    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]

    df = pd.DataFrame()
    
    for i in file_list:
        fname = pd.read_csv(file_path + i)
        df = df.append(fname)
    df = df.drop_duplicates(subset=['id'])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = feed_name + '_' + time_now + '.csv'
    upload_combined_to_gcs_raw(csv_body=csv_buffer, uploaded_filename=filename)

    return df

def upload_unique_to_gcs_from_raw_philstar(feed_name):
    file_path ='/data/philstar/'
    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]

    df = pd.DataFrame()
    
    for i in file_list:
        fname = pd.read_csv(file_path + i)
        df = df.append(fname)
    df = df.drop_duplicates(subset=['id'])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = feed_name + '_' + time_now + '.csv'
    upload_combined_to_gcs_raw(csv_body=csv_buffer, uploaded_filename=filename)

    return df


@task(task_id="inquirer_feed")
def inquirer_feed(ds=None, **kwargs):
    upload_formatted_rss_feed_rich("https://www.inquirer.net/fullfeed", "inquirer")
    return True

@task(task_id="philstar_nation")
def philstar_nation_feed(ds=None, **kwargs):
    upload_formatted_rss_feed_gerard("https://www.philstar.com/rss/nation", "philstar")
    return True

@task(task_id="dl_from_gcs_inquirer")
def dl_from_gcs_inquirer(ds=None, **kwargs):
    download_file_from_gcs(remote_files_inquirer, "/data/inquirer/")
    return True

@task(task_id="dl_from_gcs_philstar")
def dl_from_gcs_philstar(ds=None, **kwargs):
    download_file_from_gcs(remote_files_philstar, "/data/philstar/")
    return True

@task(task_id="combined_inquirer")
def combined_inquirer(ds=None, **kwargs):
    upload_unique_to_gcs_from_raw_inquirer("inquirer_combined")
    return True
    #upload_unique_to_gcs_from_raw(files, path, "inquirer_combined")

@task(task_id="combined_philstar")
def combined_philstar(ds=None, **kwargs):
    upload_unique_to_gcs_from_raw_philstar("philstar_combined")
    return True

with DAG(
    'rich_news_site_scrapers',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        # 'depends_on_past': False,
        # 'email': ['caleb@eskwelabs.com'],
        # 'email_on_failure': False,
        # 'email_on_retry': False,
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='RSS parsers',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 15),
    catchup=False,
    tags=['scrapers'],
) as dag:

    t1 = EmptyOperator(task_id="start_message")

    t_end = EmptyOperator(task_id="end_message")

    (t1 >> [inquirer_feed() >> philstar_nation_feed()]
        >> dl_from_gcs_inquirer() >> dl_from_gcs_philstar()
        >> combined_inquirer() >> combined_philstar()
        >> t_end)