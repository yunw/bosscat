import boto3
from botocore.exceptions import ClientError

from bosscat.utils import (
    client_error_code,
    get_account_id,
    get_topic_arn,
    try_client
    )


NOT_FOUND = 'NotFound'


def ensure_topic(topic_name, region):
    client = boto3.client('sns', region)
    response = client.create_topic(Name=topic_name)
    return response['TopicArn']


def destroy_topic_and_subscriptions(topic_name, region, account_id):
    client = boto3.client('sns', region)
    topic_arn = get_topic_arn(
        region,
        account_id,
        topic_name
        )
    try:
        next_token = ''
        while True:
            response = client.list_subscriptions_by_topic(
                                TopicArn = topic_arn,
                                NextToken = next_token
                                )
            for subscription in response['Subscriptions']:
                client.unsubscribe(
                    SubscriptionArn=subscription['SubscriptionArn']
                    )
            if not next_token:
                break
    except ClientError as ex:
        if client_error_code(ex) != NOT_FOUND:
            raise(ex)
    client.delete_topic(TopicArn=topic_arn)


