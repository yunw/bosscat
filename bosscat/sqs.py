import json

import boto3
from botocore.exceptions import ClientError

from bosscat.utils import client_error_code, get_queue_arn, try_client


NON_EXISTENT_QUEUE = 'AWS.SimpleQueueService.NonExistentQueue'
QUEUE_DELETED_RECENTLY = 'AWS.SimpleQueueService.QueueDeletedRecently'


def ensure_queue(queue_name, region, queue_policy=None, redrive_policy=None):
    client = boto3.client('sqs', region)
    attributes = {}
    if queue_policy:
        attributes['Policy'] = json.dumps(queue_policy)
    if redrive_policy:
        attributes['RedrivePolicy'] = json.dumps(redrive_policy)
    try_client(
        lambda: client.create_queue(
            QueueName = queue_name,
            Attributes = attributes
            )
        )


def destroy_queue(queue_name, region):
    client = boto3.client('sqs', region)
    try:
        client.delete_queue(
            QueueUrl = client.get_queue_url(QueueName=queue_name)['QueueUrl']
            )
    except ClientError as ex:
        if client_error_code(ex) != NON_EXISTENT_QUEUE:
            raise(ex)


