from copy import deepcopy
from datetime import datetime
from threading import Thread
from time import time, sleep

import boto3

from bosscat import elasticbeanstalk, iam, rds, s3, sns, sqs, utils


def configure(config):
    def config_obj(obj):
        if obj.get('dead_letter_queue'):
            config_obj(obj.get('dead_letter_queue'))
        if not obj.get('name'):
            obj['name'] = '{}-{}'.format(config['deployment_name'], obj['nametip'])
        if not obj.get('region'):
            obj['region'] = config['deployment_region']
        obj['name_camel'] = ''.join(
            [namepart.capitalize() for namepart in obj['name'].split('-')]
            )
        config['environment'][obj['setting_name']] = obj['name']
    config = deepcopy(config)
    config['deployment_name'] = '{}-{}-{}'.format(
        config['app_id'],
        config['deployment_delta'],
        config['deployment_tag']
        )
    config['account_id'] = utils.get_account_id()
    config['rds'] = config.get('rds', None)
    config['buckets'] = config.get('buckets', [])
    config['queues'] = config.get('queues', [])
    config['topics'] = config.get('topics', [])
    config['role_name'] = '{}-ec2-role'.format(config['deployment_name'])
    config['instance_profile_name'] = '{}-ec2-instance-profile'.format(
                                            config['deployment_name']
                                            )
    if config.get('web'):
        config['eb_webhead_name'] = '{}-{}'.format(
            config['deployment_name'],
            config['web']['nametip']
            )
        web_dict = dict(config['eb'])
        web_dict.update(config['web'])
        config['web'] = web_dict
    if config.get('worker'):
        config['eb_worker_name'] = '{}-{}'.format(
            config['deployment_name'],
            config['worker']['nametip']
            )
        worker_dict = dict(config['eb'])
        worker_dict.update(config['worker'])
        config['worker'] = worker_dict
    if config.get('rds'):
        config['environment']['BOSSCAT_RDS_INSTANCE_IDENTIFIER'] = \
            config['deployment_name']
    for obj in config['buckets'] + config['queues'] + config['topics']:
        config_obj(obj)
    setting_names = config['environment'].keys()
    config['environment']['BOSSCAT_ENVIRONMENT_NAMES'] = ','.join(setting_names)
    return config


def down(config, alert):
    config = configure(config)
    if config['deployment_region'] == 'local':
        alert('Running local; nothing to do.')
        return
    threads = [
        Thread(
            target = down_buckets,
            args = [config.get('buckets', []), alert]
            ),
        Thread(
            target = down_queues,
            args = [config.get('queues', []), alert]
            ),
        Thread(
            target = down_topics,
            args = [config.get('topics', []), config['account_id'], alert]
            ),
        ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    down_iam(config, alert)
    down_eb(config, alert)
    down_rds(config, alert)


def down_buckets(buckets, alert):
    client = s3.get_s3_client()
    for bucket in buckets:
        if bucket.get('permanent'):
            alert('Keeping permanent bucket {}'.format(bucket['name']))
        else:
            s3.destroy_bucket(bucket['name'])
            alert('Bucket {} is destroyed'.format(bucket['name']))


def down_eb(config, alert):
    if config.get('web'):
        elasticbeanstalk.destroy_environment(
            config['eb_webhead_name'],
            config['deployment_region']
            )
        alert(
            'Elastic Beanstalk webhead environment {} is terminating'.format(
                config['eb_webhead_name']
                )
            )
    if config.get('worker'):
        elasticbeanstalk.destroy_environment(
            config['eb_worker_name'],
            config['deployment_region']
            )
        alert(
            'Elastic Beanstalk worker environment {} is terminating'.format(
                config['eb_worker_name']
                )
            )


def down_iam(config, alert):
    iam.destroy_instance_profile(config['instance_profile_name'])
    alert('Instance Profile {} is destroyed'.format(
                        config['instance_profile_name']
                        ))
    iam.destroy_role(config['role_name'])
    alert('Role {} is destroyed'.format(config['role_name']))


def down_rds(config, alert):
    if config['rds']:
        rds.delete_instance(
            config['deployment_region'],
            config['deployment_name']
            )
        alert('RDS instance {} is destroyed'.format(config['deployment_name']))


def down_queues(queues, alert):
    def down_queue(queue, alert):
        if queue.get('permanent'):
            alert('Keeping permanent queue {}'.format(queue['name']))
        else:
            sqs.destroy_queue(queue['name'], queue['region'])
            alert('Queue {} is destroyed'.format(queue['name']))
    for queue in queues:
        if queue['dead_letter_queue']:
            down_queue(queue['dead_letter_queue'], alert)
        down_queue(queue, alert)


def down_topics(topics, account_id, alert):
    for topic in topics:
        if topic.get('permanent'):
            alert('Keeping permanent topic {}'.format(topic['name']))
        else:
            sns.destroy_topic_and_subscriptions(
                topic['name'],
                topic['region'],
                account_id
                )
            alert('Topic {} is destroyed'.format(topic['name']))


def up(config, alert):
    config = configure(config)
    if config['deployment_region'] == 'local':
        alert('Running local; nothing to do.')
        return
    up_rds(config, alert)
    threads = [
        Thread(
            target = up_buckets,
            args = [config.get('buckets', []), alert]
            ),
        Thread(
            target = up_queues,
            args = [config.get('queues', []), config['account_id'], alert]
            ),
        Thread(
            target = up_topics,
            args = [config.get('topics', []), config['account_id'], alert]
            ),
        ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    up_iam(config, alert)
    up_eb(config, alert)


def up_buckets(buckets, alert):
    for bucket in buckets:
        s3.ensure_bucket(
            bucket['name'],
            bucket['region'],
            cors_dict = s3.DEFAULT_CORS_DICT if bucket.get('cors') else None
            )
        alert('Bucket {} ready to go'.format(bucket['name']))


def up_eb(config, alert):
    if config.get('web') or config.get('worker'):
        eb_app_version_label = '{}-{}'.format(
            config['deployment_name'],
            datetime.now().strftime('%y%m%d%H%M%S')
            )
        elasticbeanstalk.upload_local_git_branch(
            config['deployment_region'],
            config['app_id'],
            'elasticbeanstalk-{}-{}'.format(
                config['deployment_region'],
                config['account_id']
                ),
            '.',
            eb_app_version_label
            )
        alert('Uploaded source bundle {}'.format(eb_app_version_label))
    if config.get('web'):
        elasticbeanstalk.create_environment(
            config['deployment_region'],
            config['app_id'],
            config['eb_webhead_name'],
            eb_app_version_label,
            config['solution_stack_name'],
            elasticbeanstalk.get_eb_option_settings(config, 'web'),
            False
            )
        alert(
            'Elastic Beanstalk webhead environment {} is launching'.format(
                config['eb_webhead_name']
                )
            )
    if config.get('worker'):
        elasticbeanstalk.create_environment(
            config['deployment_region'],
            config['app_id'],
            config['eb_worker_name'],
            eb_app_version_label,
            config['solution_stack_name'],
            elasticbeanstalk.get_eb_option_settings(config, 'worker'),
            True
            )
        alert(
            'Elastic Beanstalk worker environment {} is launching'.format(
                config['eb_worker_name']
                )
            )


def up_iam(config, alert):
    iam.ensure_role(
        config['role_name'],
        '{}-EC2InstanceProfilePolicy'.format(config['deployment_name']),
        iam.get_inline_policy_document(config)
        )
    alert('Role {} ready to go'.format(config['role_name']))
    iam.ensure_instance_profile(
        config['instance_profile_name'],
        config['role_name']
        )
    alert('Instance Profile {} ready to go'.format(config['instance_profile_name']))


def up_rds(config, alert):
    if not config['rds']:
        return
    alert('Creating RDS instance {}'.format(config['deployment_name']))
    rds.create_instance_from_snapshot(
        config['deployment_region'],
        config['deployment_name'],
        config['rds']['snapshot_name'],
        config['rds']['db_instance_type']
        )
    stime = int(time())
    while True:
        status = rds.get_instance_status(
            config['deployment_region'],
            config['deployment_name']
            )
        elapsed = int(time()) - stime
        min, sec = int(elapsed / 60), elapsed % 60
        alert('{:02d}:{:02d} -- status: {}'.format(min, sec, status))
        if status == 'available':
            break
        sleep(20)
    rds.modify_vpc_security_groups(
        config['deployment_region'],
        config['deployment_name'],
        config['rds']['security_groups']
        )
    alert('RDS instance {} ready to go'.format(config['deployment_name']))


def up_queues(queues, account_id, alert):
    for queue in queues:
        if queue['dead_letter_queue']:
            sqs.ensure_queue(
                queue['dead_letter_queue']['name'],
                queue['region'],
                )
            alert('Queue {} ready to go'.format(
                queue['dead_letter_queue']['name']
                ))
            redrive_policy = {
                "deadLetterTargetArn": sqs.get_queue_arn(
                    queue['region'],
                    account_id,
                    queue['dead_letter_queue']['name']
                    ),
                "maxReceiveCount":
                    queue['dead_letter_queue'].get('max_receive_count', 1)
                }
        else:
            redrive_policy = None
        sqs.ensure_queue(
            queue['name'],
            queue['region'],
            redrive_policy = redrive_policy
            )
        alert('Queue {} ready to go'.format(queue['name']))


def up_topics(topics, account_id, alert):
    for topic in topics:
        topic_arn = sns.ensure_topic(topic['name'], topic['region'])
        alert('Topic {} ready to go'.format(topic['name']))
        client = boto3.client('sns', topic['region'])
        for subscription in topic.get('subscriptions', []):
            client.subscribe(
                TopicArn = topic_arn,
                Protocol = subscription['protocol'],
                Endpoint = subscription['endpoint']
                )
            alert('Subscription {}: {} ready to go'.format(
                    subscription['protocol'],
                    subscription['endpoint']
                ))


