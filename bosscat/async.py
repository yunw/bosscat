import codecs
from hashlib import sha1
from importlib import import_module
import pickle
from uuid import uuid4

import boto3

from bosscat import settings


class SignatureMismatch(Exception):
    pass


class NotAsyncReceiver(Exception):
    pass


SECRET_BYTES = settings.BOSSCAT_SECRET.encode('utf-8')


encode64 = codecs.getencoder('base64')
decode64 = codecs.getdecoder('base64')


def get_worker(worker_key):
    """obtain a worker function (worker_key uses dot path notation)"""
    keyparts = worker_key.split('.')
    module_name = '.'.join(keyparts[0: -1])
    module = import_module(module_name)
    worker_function = getattr(module, keyparts[-1])
    return worker_function


def dispatch_message(msg_dict):
    """load a worker function and call it"""
    worker_function = get_worker(msg_dict['worker_key'])
    worker_function(*msg_dict['args'], **msg_dict['kwargs'])


def _get_signature(message_pickle, message_secret_bytes):
    return sha1(message_pickle + message_secret_bytes).hexdigest()


def _sns_publish(msg_dict):
    sns = boto3.client('sns', settings.DEPLOYMENT_REGION)
    sns.publish(
        TopicArn = settings.ASYNC_TOPIC_ARN,
        Subject = msg_dict['status'],
        Message = str(msg_dict)
        )


def receive_message(raw_http_content):
    """receive message handler"""
    if not settings.ASYNC_RECEIVER:
        raise NotAsyncReceiver()
    # open the message
    msg_envelope = pickle.loads(decode64(raw_http_content)[0])
    msg_pickle = msg_envelope['msg_pickle']
    msg_dict = pickle.loads(msg_pickle)
    # validate the message was signed with SECRET_BYTES
    msg_signature = _get_signature(msg_pickle, SECRET_BYTES)
    if msg_signature != msg_envelope['msg_signature']:
        # log the signature mismatch to sns
        msg_dict['status'] = 'Signature Mismatch'
        _sns_publish(msg_dict)
        raise SignatureMismatch()
    # log the message to sns
    msg_dict['status'] = 'Received'
    _sns_publish(msg_dict)
    # call the worker
    dispatch_message(msg_dict)
    msg_dict['status'] = 'Complete'
    _sns_publish(msg_dict)


class bosscat_worker(object):
    """decorator class for bosscat worker functions"""

    def __init__(self, worker_function):
        worker_key = '{}.{}'.format(
            worker_function.__module__,
            worker_function.__name__
            )
        self.worker_key = worker_key
        self.worker_function = worker_function

    def __call__(self, *args, **kwargs):
        self.worker_function(*args, **kwargs)

    def async(self, *args, **kwargs):
        self.delay(0, *args, **kwargs)

    def delay(self, delay_seconds, *args, **kwargs):
        if settings.ASYNC_RUN_LOCAL:
            self.__call__(*args, **kwargs)
        else:
            msg_dict = {
                'msg_id': str(uuid4()),
                'worker_key': self.worker_key,
                'args': args,
                'kwargs': kwargs
                }
            if delay_seconds:
                msg_dict['delay_seconds'] = delay_seconds
            # pickle the message and put it in the envelope
            # sign the envelope with SECRET_BYTES as a salt
            msg_pickle = pickle.dumps(msg_dict)
            msg_envelope = {
                'msg_pickle': msg_pickle,
                'msg_signature': _get_signature(msg_pickle, SECRET_BYTES)
                }
            # pickle the envelope and encode it to base64 because SQS
            #   rejects the control characters used by pickle; decode
            #   the bytes returned by encode64 to str to satisfy
            #   sqs.send_message
            message_body_64 = encode64(pickle.dumps(msg_envelope))[0].decode()
            sqs = boto3.client('sqs', settings.DEPLOYMENT_REGION)
            response = sqs.send_message(
                QueueUrl = settings.ASYNC_MQ_URL,
                MessageBody = message_body_64,
                DelaySeconds = delay_seconds
                )
            # log the message to sns
            msg_dict['status'] = 'Sent'
            _sns_publish(msg_dict)


class bosscat_cron(object):
    """decorator class for bosscat cron functions"""

    def __init__(self, cron_function, **kwargs):
        self.__dict__.update(kwargs)
        self.cron_function = cron_function
        self.cron_name = '{}.{}'.format(
            cron_function.__module__,
            cron_function.__name__
            )

    def __call__(self, request):
        if not settings.ASYNC_RECEIVER:
            if hasattr(self, 'http404_exception_class'):
                raise self.http404_exception_class()
            if hasattr(self, 'http404_response'):
                return self.http404_response
            raise NotAsyncReceiver()
        sns_dict = {
            'status': 'Launch Cron',
            'cron': self.cron_name,
            'uuid': str(uuid4())
            }
        _sns_publish(sns_dict)
        self.cron_function(request)
        sns_dict['status'] = 'Complete Cron'
        _sns_publish(sns_dict)
        return self.get_response()

    def get_response(self):
        return getattr(self, 'http_response', True)


