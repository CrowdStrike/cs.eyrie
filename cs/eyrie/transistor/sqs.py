from botocore.endpoint import convert_to_response_dict
from collections import OrderedDict, namedtuple
from cs.eyrie.config import MAX_TIMEOUT
from datetime import datetime
from random import uniform
from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from uuid import uuid4
from zlib import compress, decompress


ZLIB_COMPRESS = 1 << 1
BASE64_ENCODE = 1 << 2


class SQSError(Exception):

    def __init__(self, message, code, error_type, detail):
        super(SQSError, self).__init__(message)
        self.code = code
        self.error_type = error_type
        self.detail = detail

    def __str__(self):
        return self.__unicode__().encode('utf8')

    def __unicode__(self):
        txt = u'SQSError(message="{}", code="{}", error_type="{}", detail="{}")'
        return txt.format(self.message,
                          self.code,
                          self.error_type,
                          self.detail)


BatchResponse = namedtuple(
    'BatchResponse', [
        'Successful',
        'Failed',
        'ResponseMetadata',
    ],
)
DeleteMessageRequestEntry = namedtuple(
    'DeleteMessageRequestEntry', [
        'Id',
        'ReceiptHandle',
    ],
)
ReceiveMessageResponse = namedtuple(
    'ReceiveMessageResponse', [
        'Messages',
        'ResponseMetadata',
    ],
)
ResponseMetadata = namedtuple(
    'ResponseMetadata', [
        'HTTPHeaders',
        'HTTPStatusCode',
        'RequestId',
    ],
)
SQSMessage = namedtuple(
    'SQSMessage', [
        'Attributes',
        'MessageAttributes',
        'MessageId',
        'ReceiptHandle',
        'Body',
    ],
)
SendMessageRequestEntry = namedtuple(
    'SendMessageRequestEntry', [
        'Id',
        'MessageBody',
        'DelaySeconds',
        'MessageAttributes',
    ],
)


class AsyncSQSClient(object):
    """AWS client that handles asynchronous operations with an SQS queue
        * `session` should be the output of botocore.session.get_session()`
        * `queue_name` is the name of the queue to operate on
        * `queue_url` is the URL used for all queue operations
        * `region` is the AWS region the queue is in
        * `http_client` is an instance of `tornado.httpclient.AsyncHTTPClient`
    """

    api_version = '2012-11-05'
    long_poll_timeout = 20
    min_sleep = 5
    max_messages = 10
    max_timeout = MAX_TIMEOUT
    retry_attempts = 4
    # See: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html
    retry_exceptions = {
        'InternalError',
        'InternalFailure',
        'ServiceUnavailable',
        'ThrottlingException',
    }

    def __init__(self,
                 session,
                 logger,
                 queue_name=None,
                 queue_url=None,
                 region=None,
                 http_client=None):
        self.logger = logger
        self.queue_name = queue_name
        if region is None:
            region = "us-west-1"
        self.region = region
        self._session = session
        self._client = self._session.create_client(
            'sqs',
            region_name=self.region,
            api_version=self.api_version,
        )
        if queue_url is None:
            response = self._client.get_queue_url(QueueName=self.queue_name)
            queue_url = response['QueueUrl']
        self.queue_url = queue_url
        if http_client is None:
            self.http_client = AsyncHTTPClient()
        else:
            self.http_client = http_client

    @gen.coroutine
    def _execute_batch(self, op_name, nt_class, singleton_method,
                       *req_entries, **req_kwargs):
        """Asynchronously sends messages to the queue
        """
        api_params = dict(
            QueueUrl=self.queue_url,
        )
        req_entries = list(req_entries)
        result = dict(
            Successful=[],
            Failed=[],
            ResponseMetadata=[],
        )
        while req_entries:
            entries = []
            for entry in req_entries[:self.max_messages]:
                assert isinstance(entry, nt_class)
                # botocore expects dictionaries
                entries.append(vars(entry))
            api_params['Entries'] = entries
            try:
                response = yield self._operate(op_name,
                                               api_params,
                                               **req_kwargs)
            except SQSError as err:
                for entry in req_entries[:self.max_messages]:
                    try:
                        response = yield singleton_method(entry)
                    except SQSError as err:
                        log_msg = 'Too many SQS errors, retry failed: %s'
                        self.logger.error(log_msg, err)
                        result['Failed'].append(entry)
                    else:
                        result['Successful'].append(entry)
                        result['ResponseMetadata'].append(
                            response['ResponseMetadata']
                        )
            else:
                for success in response.get('Successful', []):
                    # Populate our return data with objects passed in
                    # We want this to blow up, so that inconsistencies
                    # in the response are bubbled up
                    matching_items = [
                        sre
                        for sre in req_entries
                        if sre.Id == success['Id']
                    ]
                    if len(matching_items) > 1:
                        message = 'Duplicate message IDs in batch: %s'
                        raise SQSError(
                            message=message % success['Id'],
                            code='9998',
                            error_type='ClientError',
                            detail='',
                        )
                    elif not matching_items:
                        message = 'No matching message ID for: %s'
                        raise SQSError(
                            message=message % success['Id'],
                            code='9999',
                            error_type='ClientError',
                            detail='',
                        )
                    result['Successful'].append(matching_items[0])
                result['ResponseMetadata'].append(response['ResponseMetadata'])
                for err in response.get('Failed', []):
                    entry = [
                        entry
                        for entry in req_entries
                        if entry.Id == err['Id']
                    ][0]
                    try:
                        # This will include retry logic,
                        # up to self.retry_attempts
                        response = yield singleton_method(entry)
                    except SQSError as err:
                        log_msg = 'Too many SQS errors, retry failed: %s'
                        self.logger.error(log_msg, err)
                        result['Failed'].append(entry)
                    else:
                        result['Successful'].append(entry)
                        result['ResponseMetadata'].append(
                            response['ResponseMetadata']
                        )
            req_entries = req_entries[self.max_messages:]

        raise gen.Return(BatchResponse(**result))

    @gen.coroutine
    def _operate(self, op_name, api_params, **req_kwargs):
        """Execute asynchronous transfer, possibly retrying
        """
        retry = req_kwargs.pop('retry', False)
        attempt = req_kwargs.pop('attempt', 0) + 1
        op_model = self._client.meta.service_model.operation_model(op_name)
        http_request = self._prepare_request(op_model, api_params,
                                             **req_kwargs)
        http_response = yield self.http_client.fetch(http_request,
                                                     raise_error=False)
        parsed_response = self._parse_response(op_model, http_response)
        if 'ResponseMetadata' in parsed_response:
            metadata = parsed_response.pop('ResponseMetadata')
            parsed_response['ResponseMetadata'] = ResponseMetadata(
                HTTPHeaders=metadata.get('HTTPHeaders'),
                HTTPStatusCode=int(metadata.get('HTTPStatusCode', 0)),
                RequestId=metadata.get('RequestId'),
            )
        error = parsed_response.get('Error', {})
        if http_response.code != 200 or error:
            if retry and attempt <= self.retry_attempts and \
               any([
                   ename in http_response.body
                   for ename in self.retry_exceptions
               ]):
                req_kwargs['retry'] = retry
                req_kwargs['attempt'] = attempt
                # https://www.awsarchitectureblog.com/2015/03/backoff.html
                delay = min(self.max_timeout.total_seconds(),
                            self.min_sleep * 2 ** attempt)
                delay = min(self.max_timeout.total_seconds(),
                            uniform(self.min_sleep, delay * 3))
                yield gen.sleep(delay)
                response = yield self._operate(op_name, api_params,
                                               **req_kwargs)
                raise gen.Return(response)
            else:
                # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html
                if http_response.code == 599:
                    error.setdefault('Message', 'API call timeout')
                    error.setdefault('Detail', op_name)
                raise SQSError(error.get('Message', ''),
                               error.get('Code', ''),
                               error.get('Type', ''),
                               error.get('Detail', ''))

        raise gen.Return(parsed_response)

    def _parse_attributes(self, attributes):
        result = {}
        for att_name, att_val in attributes.items():
            if att_name.endswith('Timestamp'):
                # SentTimestamp and ApproximateFirstReceiveTimestamp are
                # each returned as an integer representing the epoch time
                # in milliseconds.
                att_val = datetime.utcfromtimestamp(int(att_val)/1000.0)
            elif att_name.endswith('Count'):
                att_val = int(att_val)
            result[att_name] = att_val
        return result

    def _parse_msg_attributes(self, msg_attributes):
        result = {}
        for att_name, att_dict in msg_attributes.items():
            att_val = att_dict.get('StringValue',
                                   att_dict.get('BinaryValue'))
            if att_dict['DataType'] == 'Number':
                att_val = float(att_val)
                if att_val.is_integer():
                    att_val = int(att_val)
            result[att_name] = att_val
        return result

    def _parse_response(self, op_model, http_response):
        """Use botocore to parse a response from AWS, returning
        HTTPRequest.
        """
        # The following reverse-engineerd from:
        # botocore.endpoint.Endpoint._get_response
        # Mimic requests' Response
        http_response.content = http_response.body or u''
        http_response.status_code = http_response.code
        response_dict = convert_to_response_dict(http_response,
                                                 op_model)
        parser = self._client._endpoint._response_parser_factory.create_parser(
            op_model.metadata['protocol']
        )
        return parser.parse(response_dict,
                            op_model.output_shape)

    def _prepare_request(self, op_model, api_params, **req_kwargs):
        """Use botocore to sign a request to AWS, and convert to Tornado's
        HTTPRequest.
        """
        # The following reverse engineered from:
        # botocore.client.BaseClient._make_api_call
        request_context = {
            'client_region': self._client.meta.region_name,
            'client_config': self._client.meta.config,
            'has_streaming_input': op_model.has_streaming_input,
            'auth_type': op_model.auth_type,
        }
        request_dict = self._client._convert_to_request_dict(
            api_params,
            op_model,
            context=request_context)
        # This adds all request bits necessary for authenticating to AWS
        aws_request = self._client._endpoint.create_request(request_dict,
                                                            op_model)
        req_kwargs['headers'] = aws_request.headers
        req_kwargs['method'] = aws_request.method
        req_kwargs['body'] = aws_request.body
        return HTTPRequest(aws_request.url, **req_kwargs)

    @gen.coroutine
    def delete_message(self, sqs_message, **req_kwargs):
        """Asynchronously deletes a message from the queue
        """
        req_kwargs.setdefault('retry', True)
        assert isinstance(sqs_message, (SQSMessage, DeleteMessageRequestEntry))
        # botocore expects dictionaries
        api_params = dict(
            QueueUrl=self.queue_url,
            ReceiptHandle=sqs_message.ReceiptHandle,
        )
        api_params.pop('Id', None)

        response = yield self._operate(
            'DeleteMessage',
            api_params,
            **req_kwargs
        )
        raise gen.Return(response)

    @gen.coroutine
    def delete_message_batch(self, *sqs_messages, **req_kwargs):
        """Asynchronously deletes messages from the queue
        """
        req_kwargs.setdefault('retry', True)
        batch = []
        for item in sqs_messages:
            if isinstance(item, DeleteMessageRequestEntry):
                batch.append(item)
            else:
                assert isinstance(item, SQSMessage)
                batch.append(DeleteMessageRequestEntry(
                    Id=uuid4().hex,
                    ReceiptHandle=item.ReceiptHandle,
                ))

        batch_response = yield self._execute_batch(
            'DeleteMessageBatch',
            DeleteMessageRequestEntry,
            self.delete_message,
            *batch,
            **req_kwargs)
        raise gen.Return(batch_response)

    @gen.coroutine
    def receive_message_batch(self, **req_kwargs):
        """Asynchronously receive messages from the queue
        """
        req_kwargs.setdefault('retry', True)
        max_messages = req_kwargs.pop('max_messages', self.max_messages)
        response = yield self._operate(
            'ReceiveMessage',
            dict(
                AttributeNames=['All'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=max_messages,
                QueueUrl=self.queue_url,
                WaitTimeSeconds=self.long_poll_timeout,
            ),
            **req_kwargs
        )

        messages = []
        incoming = response.get('Messages', []) or []
        for msg in incoming:
            msg_attr = self._parse_msg_attributes(msg.get('MessageAttributes',
                                                          {}))
            flags = msg_attr.get('flags', 0)
            body = msg['Body']
            if flags & BASE64_ENCODE:
                body = body.decode('base64')
            if flags & ZLIB_COMPRESS:
                body = decompress(body)

            messages.append(
                SQSMessage(
                    Attributes=self._parse_attributes(msg.get('Attributes',
                                                              {})),
                    MessageAttributes=msg_attr,
                    MessageId=msg['MessageId'],
                    ReceiptHandle=msg['ReceiptHandle'],
                    Body=body,
                )
            )

        raise gen.Return(ReceiveMessageResponse(
            Messages=messages,
            ResponseMetadata=response['ResponseMetadata'],
        ))

    @gen.coroutine
    def send_message(self, req_entry, **req_kwargs):
        """Asynchronously sends a message to the queue
        """
        req_kwargs.setdefault('retry', True)
        assert isinstance(req_entry, SendMessageRequestEntry)
        # botocore expects dictionaries
        api_params = dict(
            vars(req_entry),
            QueueUrl=self.queue_url,
        )
        api_params.pop('Id', None)

        response = yield self._operate(
            'SendMessage',
            api_params,
            **req_kwargs
        )
        raise gen.Return(response)

    @gen.coroutine
    def send_message_batch(self, *req_entries, **req_kwargs):
        """Asynchronously sends messages to the queue
        """
        req_kwargs.setdefault('retry', True)
        batch_response = yield self._execute_batch('SendMessageBatch',
                                                   SendMessageRequestEntry,
                                                   self.send_message,
                                                   *req_entries, **req_kwargs)
        raise gen.Return(batch_response)


def build_send_message_request(message_body,
                               delay_seconds=0,
                               binary=True,
                               **message_attributes):
    """Construct a namedtuple representing a SendMessageRequestEntry,
    correctly handling binary input data. This assumes this class
    will be responsible for reading these messages back from the queue.
    """
    MessageAttributes = OrderedDict()
    flags = 0
    if binary:
        flags |= ZLIB_COMPRESS
        flags |= BASE64_ENCODE
        message_body = compress(message_body).encode('base64')
    MessageAttributes['flags'] = dict(
        DataType='Number',
        StringValue=str(flags),
    )
    MessageAttributes.update(message_attributes)
    return SendMessageRequestEntry(
        Id=uuid4().hex,
        MessageBody=message_body,
        DelaySeconds=int(delay_seconds),
        MessageAttributes=MessageAttributes
    )


def deserialize_send_message_request(frames):
    """Deserialize a SendMessageRequestEntry from frames on a ZMQ socket.
    """
    from json import loads
    id_pos, body_pos, delay_pos, attr_pos = [
        SendMessageRequestEntry._fields.index(attr_name)
        for attr_name in ('Id',
                          'MessageBody',
                          'DelaySeconds',
                          'MessageAttributes')
    ]
    return SendMessageRequestEntry(
        Id=frames[id_pos],
        MessageBody=frames[body_pos],
        DelaySeconds=int(frames[delay_pos]),
        MessageAttributes=loads(frames[attr_pos]),
    )


def serialize_send_message_request(req):
    """Serialize a SendMessageRequestEntry to frames suitable to
    be sent over the wire on a ZMQ socket.
    """
    from json import dumps
    assert isinstance(req, SendMessageRequestEntry)
    return SendMessageRequestEntry(
        Id=req.Id,
        MessageBody=req.MessageBody,
        DelaySeconds=str(req.DelaySeconds),
        MessageAttributes=dumps(req.MessageAttributes)
    )
