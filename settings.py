import os
import json

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from bosscat import utils


def _get_secrets(secrets_bucket_name, app_id, delta, tag):
    s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
    # load common secrets
    common_secrets_filename = '{}-secrets.json'.format(app_id)
    obj = s3.Object(secrets_bucket_name, common_secrets_filename).get()
    secrets = json.loads((obj['Body'].read().decode()))
    # load untagged secrets
    untagged_secrets_filename = '{}-{}-secrets.json'.format(app_id, delta)
    obj = s3.Object(secrets_bucket_name, untagged_secrets_filename).get()
    secrets.update(json.loads((obj['Body'].read().decode())))
    # load tagged secrets
    try:
        tagged_secrets_filename = '{}-{}-{}-secrets.json'.format(
            app_id,
            delta,
            tag
            )
        obj = s3.Object(secrets_bucket_name, tagged_secrets_filename).get()
        secrets.update(json.loads((obj['Body'].read().decode())))
    except ClientError:
        pass
    return secrets


# import bosscat environment
APP_ID = os.environ['BOSSCAT_APP_ID']
DEPLOYMENT_DELTA = os.environ['BOSSCAT_DEPLOYMENT_DELTA']
DEPLOYMENT_TAG = os.environ['BOSSCAT_DEPLOYMENT_TAG']
DEPLOYMENT_REGION = os.environ['BOSSCAT_DEPLOYMENT_REGION']
DEPLOYMENT_TIER = os.environ.get('BOSSCAT_DEPLOYMENT_TIER')
# get aws account id
AWS_ACCOUNT_ID = utils.get_account_id()
# import config environment
for env_name in os.environ['BOSSCAT_ENVIRONMENT_NAMES'].split(','):
    if env_name:
        globals()[env_name] = os.environ[env_name]
del env_name
# set deployment name
DEPLOYMENT_NAME = '{}-{}-{}'.format(
    APP_ID,
    DEPLOYMENT_DELTA,
    DEPLOYMENT_TAG
    )
# Get deployment secrets
globals().update(
    _get_secrets(
        os.environ['BOSSCAT_SECRETS_BUCKET'],
        APP_ID,
        DEPLOYMENT_DELTA,
        DEPLOYMENT_TAG
        )
    )
# async settings
if 'ASYNC_TOPIC_NAME' in globals():
    ASYNC_TOPIC_ARN = 'arn:aws:sns:{}:{}:{}'.format(
                                        DEPLOYMENT_REGION,
                                        AWS_ACCOUNT_ID,
                                        ASYNC_TOPIC_NAME
                                        )
if 'ASYNC_MQ_NAME' in globals():
    ASYNC_MQ_ARN = 'arn:aws:sqs:{}:{}:{}'.format(
                                        DEPLOYMENT_REGION,
                                        AWS_ACCOUNT_ID,
                                        ASYNC_MQ_NAME
                                        )
    ASYNC_MQ_URL = 'https://sqs.{}.amazonaws.com/{}/{}'.format(
                                        DEPLOYMENT_REGION,
                                        AWS_ACCOUNT_ID,
                                        ASYNC_MQ_NAME
                                        )
if 'ASYNC_DQL_NAME' in globals():
    ASYNC_DLQ_ARN = 'arn:aws:sqs:{}:{}:{}'.format(
                                        DEPLOYMENT_REGION,
                                        AWS_ACCOUNT_ID,
                                        ASYNC_DLQ_NAME
                                        )
# Async defaults
ASYNC_RECEIVER = (DEPLOYMENT_TIER == 'worker')
ASYNC_RUN_LOCAL = False


