import os
import subprocess

import boto3
from botocore.exceptions import ClientError

from bosscat.s3 import upload_local_file_to_bucket
from bosscat.utils import client_error_code, getenv, try_client


INVALID_PARAMETER_VALUE = 'InvalidParameterValue'


webhead_tier_dict = {
    'Name': 'WebServer',
    'Type': 'Standard'
    }


worker_tier_dict = {
    'Name': 'Worker',
    'Type': 'SQS/HTTP'
    }


def create_application_version(
            region,
            app_id,
            source_bucket_name,
            bundle_name,
            bundle_description
            ):
    eb = boto3.client('elasticbeanstalk', region)
    try_client(
        lambda: eb.create_application_version(
            ApplicationName = app_id,
            VersionLabel = bundle_name,
            Description = bundle_description,
            SourceBundle = {
                'S3Bucket': source_bucket_name,
                'S3Key': '{}/{}.zip'.format(app_id, bundle_name)
                }
            )
        )


def create_environment(
            region,
            app_id,
            environment_name,
            version_label,
            solution_stack_name,
            option_settings,
            is_worker_tier = False
            ):
    if is_worker_tier:
        tier_dict = worker_tier_dict
    else:
        tier_dict = webhead_tier_dict
    eb = boto3.client('elasticbeanstalk', region)
    try_client(
        lambda: eb.create_environment(
            ApplicationName = app_id,
            EnvironmentName = environment_name,
            SolutionStackName = solution_stack_name,
            VersionLabel = version_label,
            OptionSettings = option_settings,
            Tier = tier_dict,
            Tags = [{'Key': 'bosscat', 'Value': environment_name}]
            )
        )


def destroy_environment(environment_name, region):
    eb = boto3.client('elasticbeanstalk', region)
    try:
        eb.terminate_environment(EnvironmentName=environment_name)
    except ClientError as ex:
        if client_error_code(ex) != INVALID_PARAMETER_VALUE:
            raise(ex)


def get_eb_option_settings(config, tier):
    eb = config[tier]
    environment_dict = getenv(config, tier)
    environment_dict.update(config['environment'])
    option_dict = {
        "aws:elasticbeanstalk:environment": {
            "ServiceRole": eb['service_role']
            },
        "aws:autoscaling:launchconfiguration": {
            "EC2KeyName": eb['ssh_key_name'],
            "IamInstanceProfile": config['instance_profile_name'],
            "InstanceType": eb['instance_type'],
            "SSHSourceRestriction": "tcp,22,22,{}".format(
                                            eb['security_groups'][0]
                                            ),
            "SecurityGroups": ", ".join(eb['security_groups'])
            },
        "aws:elasticbeanstalk:application": {
            "Application Healthcheck URL": eb['healthcheck_url']
            },
        "aws:elasticbeanstalk:container:python": {
            "NumProcesses": eb['num_processes'],
            "NumThreads": eb['num_threads'],
            "WSGIPath": eb['wsgi_path']
            },
        "aws:autoscaling:asg": {
            "MinSize": eb['minimum_instance_count'],
            "MaxSize": eb['maximum_instance_count']
            },
        "aws:elasticbeanstalk:application:environment": environment_dict
        }
    if tier == 'worker':
        queue_name = '{}-{}'.format(
                config['deployment_name'],
                'bosscat-mq'
                )
        option_dict["aws:elasticbeanstalk:sqsd"] = {
            "WorkerQueueURL": 'https://sqs.{}.amazonaws.com/{}/{}'.format(
                                            config['deployment_region'],
                                            config['account_id'],
                                            queue_name
                                            ),
            "HttpPath": eb['receive_url'],
            "MimeType": "text/plain",
            }
    option_settings = []
    for namespace_item in option_dict.items():
        for option_item in namespace_item[1].items():
            eb_option = {
                'Namespace': namespace_item[0],
                'OptionName': option_item[0],
                'Value': option_item[1]
                }
            option_settings.append(eb_option)
    return option_settings


def upload_local_git_branch(
            region,
            app_id,
            source_bucket_name,
            tmp_dir,
            bundle_name
            ):
    filename = os.path.join(tmp_dir, '{}.zip'.format(bundle_name))
    subprocess.call(['git', 'archive', '-o', filename, 'HEAD'])
    git_log_proc = subprocess.Popen(['git', 'log', '-n', '1'], stdout=subprocess.PIPE)
    try:
        upload_local_file_to_bucket(
            filename,
            source_bucket_name,
            '{}/{}.zip'.format(app_id, bundle_name)
            )
    finally:
        os.unlink(filename)
    create_application_version(
        region,
        app_id,
        source_bucket_name,
        bundle_name,
        git_log_proc.stdout.read().decode()[:200]
        )


