from copy import deepcopy
import json

import boto3
from botocore.exceptions import ClientError

from bosscat import utils


ENTITY_ALREADY_EXISTS = 'EntityAlreadyExists'
MALFORMED_POLICY = 'MalformedPolicy'
NO_SUCH_ENTITY = 'NoSuchEntity'


BASE_INLINE_POLICY_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "MetricsAccess",
            "Action": [
                "cloudwatch:PutMetricData"
                ],
            "Effect": "Allow",
            "Resource": "*"
            },
        {
            "Sid": "ElasticBeanstalkBucketAccess",
            "Action": [
                "s3:Get*",
                "s3:List*",
                "s3:PutObject"
                ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::elasticbeanstalk-*",
                "arn:aws:s3:::elasticbeanstalk-*/*"
                ]
            },
        {
            "Sid": "DynamoPeriodicTasks",
            "Action": [
                "dynamodb:BatchGetItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:DeleteItem",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:UpdateItem"
                ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:dynamodb:*:*:table/*-stack-AWSEBWorkerCronLeaderRegistry*"
                ]
            },
        ]
    }


DEFAULT_ASSUME_ROLE_POLICY_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
                },
            "Action": "sts:AssumeRole"
            }
        ]
    }


def get_inline_policy_document(config):
    aws_account_id = utils.get_account_id()
    ipd = deepcopy(BASE_INLINE_POLICY_DOCUMENT)
    ipd_statement = ipd["Statement"]
    ipd_statement.append({
        "Sid": "SecretsBucketAccess",
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::{}-secrets/*".format(config["app_id"]),
        })
    for bucket in config.get("buckets", []):
        statement = {
            "Sid": "BucketAccess{}".format(bucket['name_camel']),
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                utils.get_bucket_arn(bucket["name"]),
                "{}/*".format(utils.get_bucket_arn(bucket["name"])),
                ]
            }
        ipd_statement.append(statement)
    for queue in config.get("queues", []):
        statement = {
            "Sid": "QueueAccess{}".format(queue['name_camel']),
            "Effect": "Allow",
            "Action": "sqs:*",
            "Resource": utils.get_queue_arn(
                            queue["region"],
                            aws_account_id,
                            queue["name"]
                            ),
            }
        ipd_statement.append(statement)
        dlq = queue.get("dead_letter_queue")
        if dlq:
            statement = {
                "Sid": "QueueAccess{}".format(dlq['name_camel']),
                "Effect": "Allow",
                "Action": "sqs:*",
                "Resource": utils.get_queue_arn(
                                queue["region"],
                                aws_account_id,
                                dlq["name"]
                                ),
                }
            ipd_statement.append(statement)
    for topic in config.get("topics", []):
        statement = {
            "Sid": "TopicPublishAccess{}".format(topic['name_camel']),
            "Effect": "Allow",
            "Action": "sns:Publish",
            "Resource": utils.get_topic_arn(
                            topic["region"],
                            aws_account_id,
                            topic["name"]
                            ),
            }
        ipd_statement.append(statement)
    return ipd


def ensure_instance_profile(instance_profile_name, role_name):
    client = boto3.client('iam')
    created = utils.try_client(
        lambda: client.create_instance_profile(
            InstanceProfileName = instance_profile_name
            ),
        ignore = [ENTITY_ALREADY_EXISTS]
        )
    if created:
        utils.try_client(
            lambda: client.add_role_to_instance_profile(
                InstanceProfileName = instance_profile_name,
                RoleName = role_name
                )
            )


def ensure_role(
            role_name,
            policy_name,
            policy_document,
            assume_role_policy_document = DEFAULT_ASSUME_ROLE_POLICY_DOCUMENT
            ):
    client = boto3.client('iam')
    utils.try_client(
        lambda: client.create_role(
            RoleName = role_name,
            AssumeRolePolicyDocument = json.dumps(assume_role_policy_document)
            ),
        ignore = [ENTITY_ALREADY_EXISTS]
        )
    utils.try_client(
        lambda: client.put_role_policy(
            RoleName = role_name,
            PolicyName = policy_name,
            PolicyDocument = json.dumps(policy_document)
            )
        )


def destroy_instance_profile(instance_profile_name):
    client = boto3.client('iam')
    try:
        ip = client.get_instance_profile(InstanceProfileName=instance_profile_name)
        for role in ip['InstanceProfile']['Roles']:
            client.remove_role_from_instance_profile(
                InstanceProfileName = instance_profile_name,
                RoleName = role['RoleName']
                )
        client.delete_instance_profile(InstanceProfileName=instance_profile_name)
    except ClientError as ex:
        if utils.client_error_code(ex) != NO_SUCH_ENTITY:
            raise(ex)


def destroy_role(role_name):
    client = boto3.client('iam')
    try:
        for name in client.list_role_policies(RoleName=role_name)['PolicyNames']:
            client.delete_role_policy(
                RoleName = role_name,
                PolicyName = name
                )
        client.delete_role(RoleName=role_name)
    except ClientError as ex:
        if utils.client_error_code(ex) != NO_SUCH_ENTITY:
            raise(ex)


