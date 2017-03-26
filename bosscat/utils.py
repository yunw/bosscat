from time import sleep

import boto3
from botocore.exceptions import ClientError


client_error_code = lambda ex: ex.response['Error']['Code']


def try_client(lambda_func, max_attempts=10, sleep_time=1.0, ignore=[]):
    for attempt in range(1, max_attempts + 1):
        try:
            lambda_func()
            return True
        except ClientError as ex:
            if client_error_code(ex) in ignore:
                break
            if (attempt == max_attempts):
                raise(ex)
            sleep(sleep_time)
    return False


def get_account_id():
    return boto3.client('sts').get_caller_identity()['Account']


def get_bucket_arn(bucket_name):
    return "arn:aws:s3:::{}".format(bucket_name)


def get_queue_arn(region, account_id, queue_name):
    return "arn:aws:sqs:{}:{}:{}".format(region, account_id, queue_name)


def get_topic_arn(region, account_id, topic_name):
    return "arn:aws:sns:{}:{}:{}".format(region, account_id, topic_name)


def getenv(config, deployment_tier):
    env = dict(config['environment'])
    env.update({
        'BOSSCAT_APP_ID': config['app_id'],
        'BOSSCAT_DEPLOYMENT_DELTA': config['deployment_delta'],
        'BOSSCAT_DEPLOYMENT_TAG': config['deployment_tag'],
        'BOSSCAT_DEPLOYMENT_REGION': config['deployment_region'],
        'BOSSCAT_DEPLOYMENT_TIER': deployment_tier,
        'BOSSCAT_SECRETS_BUCKET': config['secrets_bucket'],
        })
    return env


