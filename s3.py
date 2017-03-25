import boto3
from botocore.exceptions import ClientError

from bosscat.utils import client_error_code, get_bucket_arn, try_client

NO_SUCH_BUCKET = 'NoSuchBucket'
BUCKET_ALREADY_EXISTS = 'BucketAlreadyExists'
BUCKET_ALREADY_OWNED_BY_YOU = 'BucketAlreadyOwnedByYou'


DEFAULT_CORS_DICT = {
    'CORSRules': [
        {
            'AllowedHeaders': ['Authorization'],
            'AllowedMethods': ['GET'],
            'AllowedOrigins': ['*'],
            'ExposeHeaders': [],
            'MaxAgeSeconds': 3000
            }
        ]
    }


def ensure_bucket(bucket_name, region, bucket_policy=None, cors_dict=None):
    client = get_s3_client()
    if region == 'us-east-1':
        region = None
    try:
        if region:
            bucket = client.create_bucket(
                Bucket = bucket_name,
                CreateBucketConfiguration = {
                    'LocationConstraint': region
                    }
                )
        else:
            bucket = client.create_bucket(Bucket=bucket_name)
    except ClientError as ex:
        if client_error_code(ex) != BUCKET_ALREADY_OWNED_BY_YOU:
            raise
    if cors_dict:
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_dict)
    if bucket_policy:
        try_client(
            lambda: client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=bucket_policy
                )
            )


def destroy_bucket(bucket_name):
    client = get_s3_client()
    try:
        while True:
            object_list = client.list_objects(Bucket=bucket_name)
            if not object_list.get('Contents'):
                break
            delete_list = [
                {'Key': obj['Key']}
                for obj in object_list['Contents']
                ]
            client.delete_objects(
                Bucket = bucket_name,
                Delete = {
                    "Objects": delete_list,
                    "Quiet": True
                    }
                )
        client.delete_bucket(Bucket=bucket_name)
    except ClientError as ex:
        if client_error_code(ex) != NO_SUCH_BUCKET:
            raise(ex)


def get_bucket_region(bucket_name):
    client = get_s3_client()
    try:
        response = client.get_bucket_location(Bucket=bucket_name)
        region = response.get('LocationConstraint')
        return region or 'us-east-1'
    except ClientError as ex:
        if client_error_code(ex) != NO_SUCH_BUCKET:
            raise(ex)
    return None


def get_s3_client():
    return boto3.client(
        's3',
        config = boto3.session.Config(signature_version='s3v4')
        )


def upload_local_file_to_bucket(local_filename, bucket_name, key):
    client = get_s3_client()
    try_client(
        lambda: client.upload_file(local_filename, bucket_name, key)
        )


