import logging
import mock
import os
import unittest
from StringIO import StringIO
from botocore.parsers import ResponseParserError
from botocore.session import get_session
from copy import copy
from cs.eyrie import config
from cs.eyrie.transistor.sqs import (
    BASE64_ENCODE, ZLIB_COMPRESS,
    AsyncSQSClient, SQSError, BatchResponse, DeleteMessageRequestEntry,
    ReceiveMessageResponse, ResponseMetadata, SQSMessage,
    SendMessageRequestEntry,
    build_send_message_request, deserialize_send_message_request,
    serialize_send_message_request,
)
from datetime import datetime, timedelta
from json import dumps
from tornado.concurrent import Future
from tornado.httpclient import HTTPResponse, HTTPRequest, HTTPError
from tornado.testing import (
    AsyncTestCase, AsyncHTTPClient,
    gen_test, main
)
from urlparse import parse_qs
from uuid import uuid4
from zlib import compress


class AsyncSQSBase(AsyncTestCase):
    """Responsible for bootstrapping an AsyncSQSClient in such a way
    so that subclasses can easily mock out HTTP responses.
    """

    def _read_data(self, filename):
        full_path = os.path.join(os.path.dirname(__file__),
                                 'data',
                                 filename)
        return open(full_path).read()

    def setUp(self):
        super(AsyncSQSBase, self).setUp()
        self.botocore_logger = logging.getLogger('botocore')
        self.old_botocore_level = self.botocore_logger.getEffectiveLevel()
        self.botocore_logger.setLevel(logging.WARNING)
        self.http_client = AsyncHTTPClient(
            self.io_loop,
            force_instance=True,
            defaults=dict(
                request_timeout=AsyncSQSClient.long_poll_timeout+5,
            )
        )
        sqs_logger = logging.getLogger('cs.eyrie')
        sqs_logger.setLevel(logging.CRITICAL)
        self.sqs_client = AsyncSQSClient(
            get_session(),
            sqs_logger,
            queue_name='test',
            queue_url='https://foo.bar.baz',
            http_client=self.http_client,
        )
        self.old_sleep = self.sqs_client.min_sleep
        self.old_timeout = self.sqs_client.max_timeout
        self.sqs_client.min_sleep = 0.01
        self.sqs_client.max_timeout = timedelta(milliseconds=1)

    def tearDown(self):
        self.sqs_client.min_sleep = self.old_sleep
        self.sqs_client.max_timeout = self.old_timeout
        self.botocore_logger.setLevel(self.old_botocore_level)


def _setup_fetch(fetch_mock, status_code, body=None):

    def side_effect(request, **kwargs):
        if request is not HTTPRequest:
            request = HTTPRequest(request)
        buf = StringIO(body)
        response = HTTPResponse(request, status_code, None, buf)
        future = Future()
        future.set_result(response)
        return future

    fetch_mock.side_effect = side_effect


class MockTestCase(AsyncTestCase):

    @gen_test
    def runTest(self):
        """Ensure our mocking of AsyncHTTPClient functions
        """
        client = AsyncHTTPClient()
        with mock.patch.object(client, 'fetch') as fetch_mock:
            static_body = 'hello'
            static_response_code = 999
            _setup_fetch(fetch_mock, static_response_code, static_body)
            response = yield client.fetch('http://example.com')
            self.assertEqual(response.code, static_response_code)
            self.assertEqual(response.body, static_body)


class TestUtilityFunctions(unittest.TestCase):

    def test_send_builder_type(self):
        send_message_request = build_send_message_request('baz')
        self.assertIsInstance(send_message_request, SendMessageRequestEntry)

    def test_send_builder_binary(self):
        body = 'baz'
        send_message_request = build_send_message_request(body, binary=True)
        flags = send_message_request.MessageAttributes['flags']['StringValue']
        flags = int(flags)
        self.assertTrue(flags & BASE64_ENCODE)
        self.assertTrue(flags & ZLIB_COMPRESS)
        message_body = compress(body).encode('base64')
        self.assertEqual(send_message_request.MessageBody, message_body)

    def test_send_builder_nonbinary(self):
        send_message_request = build_send_message_request('baz', binary=False)
        flags = send_message_request.MessageAttributes['flags']['StringValue']
        flags = int(flags)
        self.assertFalse(flags & BASE64_ENCODE)
        self.assertFalse(flags & ZLIB_COMPRESS)

    def test_serializer_type(self):
        send_message_request = build_send_message_request('baz')
        serialized_req = serialize_send_message_request(send_message_request)
        # This seems weird, but zmq frames are a tuple so it makes sense to
        # use the same type but just ensure that all fields are strings.
        self.assertIsInstance(serialized_req, SendMessageRequestEntry)
        for fname in serialized_req._fields:
            self.assertIsInstance(getattr(serialized_req, fname), basestring)

    def test_serializer_message_attributes(self):
        send_message_request = build_send_message_request('baz')
        serialized_req = serialize_send_message_request(send_message_request)
        self.assertEqual(serialized_req.MessageAttributes,
                         dumps(send_message_request.MessageAttributes))

    def test_deserializer_type(self):
        send_message_request = build_send_message_request('baz')
        serialized_req = serialize_send_message_request(send_message_request)
        deserialized_req = deserialize_send_message_request(serialized_req)
        self.assertIsInstance(deserialized_req, SendMessageRequestEntry)

    def test_deserializer_equality(self):
        send_message_request = build_send_message_request('baz')
        serialized_req = serialize_send_message_request(send_message_request)
        deserialized_req = deserialize_send_message_request(serialized_req)
        self.assertEqual(deserialized_req, send_message_request)


class TestReceiveMessage(AsyncSQSBase):

    @gen_test
    def test_type(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('receive_message_batch_single.xml'))
            response = yield self.sqs_client.receive_message_batch()
            self.assertIsInstance(response, ReceiveMessageResponse)

    @gen_test
    def test_invalid_response(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock, 200, 'hello')
            with self.assertRaises(ResponseParserError):
                yield self.sqs_client.receive_message_batch()

    @gen_test
    def test_parsed_attributes(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('receive_message_batch_single.xml'))
            response = yield self.sqs_client.receive_message_batch()
            self.assertTrue(len(response.Messages), 1)
            attr = response.Messages[0].Attributes
            for ts_name in ('ApproximateFirstReceiveTimestamp',
                            'SentTimestamp'):
                self.assertIn(ts_name, attr)
                self.assertIsInstance(attr[ts_name], datetime)
            self.assertIn('ApproximateReceiveCount', attr)
            self.assertIsInstance(attr['ApproximateReceiveCount'], int)

    @gen_test
    def test_parsed_message_attributes(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('receive_message_batch_single.xml'))
            response = yield self.sqs_client.receive_message_batch()
            self.assertTrue(len(response.Messages), 1)
            msg_attr = response.Messages[0].MessageAttributes
            self.assertIn('flags', msg_attr)
            self.assertIsInstance(msg_attr['flags'], int)

    @gen_test
    def test_list(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('receive_message_batch_single.xml'))
            response = yield self.sqs_client.receive_message_batch()
            self.assertTrue(len(response.Messages), 1)

    @gen_test
    def test_limit(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('receive_message_batch_single.xml'))
            # In this case, we don't care about the return value.
            # We only care that we passed the right parameter
            yield self.sqs_client.receive_message_batch(max_messages=1)
            request = fetch_mock.call_args[0][0]
            params = parse_qs(request.body)
            self.assertIn('MaxNumberOfMessages', params)
            self.assertEqual(params['MaxNumberOfMessages'][0], '1')

    @gen_test
    def test_retry(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         500,
                         self._read_data('sqs_error_response.xml'))
            with self.assertRaises(SQSError):
                yield self.sqs_client.receive_message_batch(retry=True)
            self.assertEqual(fetch_mock.call_count,
                             self.sqs_client.retry_attempts+1)


class TestDeleteMessage(AsyncSQSBase):

    def setUp(self):
        super(TestDeleteMessage, self).setUp()
        self.sqs_message = SQSMessage(
            Attributes={},
            MessageAttributes={},
            MessageId=uuid4().hex,
            ReceiptHandle='foo',
            Body='bar',
        )

    @gen_test
    def test_call(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('delete_message.xml'))
            response = yield self.sqs_client.delete_message(self.sqs_message)
            self.assertEqual(response['ResponseMetadata'].HTTPStatusCode, 200)

    @gen_test
    def test_invalid_response(self):
        """Ensure all API methods raise ResponseParserError on invalid data
        """
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock, 200, 'hello')
            with self.assertRaises(ResponseParserError):
                yield self.sqs_client.delete_message(self.sqs_message)

    @gen_test
    def test_retry(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         500,
                         self._read_data('sqs_error_response.xml'))
            with self.assertRaises(SQSError):
                yield self.sqs_client.delete_message(self.sqs_message,
                                                     retry=True)
            self.assertEqual(fetch_mock.call_count,
                             self.sqs_client.retry_attempts+1)


class TestDeleteMessageBatch(AsyncSQSBase):

    def setUp(self):
        super(TestDeleteMessageBatch, self).setUp()
        self.sqs_message = SQSMessage(
            Attributes={},
            MessageAttributes={},
            MessageId='f6724f54-4e83-403e-97d5-9930dbdaa2ba',
            ReceiptHandle='foo',
            Body='bar',
        )
        self.batch = [
            DeleteMessageRequestEntry(Id='msg1', ReceiptHandle='foo'),
            DeleteMessageRequestEntry(Id='msg2', ReceiptHandle='foo'),
        ]

    @gen_test
    def test_call(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('delete_message_batch.xml'))
            response = yield self.sqs_client.delete_message_batch(*self.batch)
            self.assertEqual(response.Successful, self.batch)

    @gen_test
    def test_invalid_response(self):
        """Ensure all API methods raise ResponseParserError on invalid data
        """
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock, 200, 'hello')

            with self.assertRaises(ResponseParserError):
                yield self.sqs_client.delete_message_batch(*self.batch)

    @gen_test
    def test_retry(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         500,
                         self._read_data('sqs_error_response.xml'))
            yield self.sqs_client.delete_message_batch(*self.batch,
                                                       retry=True)
            total_requests = (
                # Retry each message in the batch
                ((self.sqs_client.retry_attempts + 1) * len(self.batch)) +
                # Retry the entire operation
                self.sqs_client.retry_attempts + 1
            )
            self.assertEqual(fetch_mock.call_count, total_requests)


class TestSendMessage(AsyncSQSBase):

    def setUp(self):
        super(TestSendMessage, self).setUp()
        self.send_message_request = build_send_message_request('baz')

    @gen_test
    def test_call(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('send_message.xml'))
            response = yield self.sqs_client.send_message(
                self.send_message_request
            )
            self.assertEqual(response['ResponseMetadata'].HTTPStatusCode, 200)

    @gen_test
    def test_invalid_response(self):
        """Ensure all API methods raise ResponseParserError on invalid data
        """
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock, 200, 'hello')
            with self.assertRaises(ResponseParserError):
                yield self.sqs_client.send_message(
                    self.send_message_request
                )

    @gen_test
    def test_retry(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         500,
                         self._read_data('sqs_error_response.xml'))
            with self.assertRaises(SQSError):
                yield self.sqs_client.send_message(
                    self.send_message_request,
                    retry=True
                )
            self.assertEqual(fetch_mock.call_count,
                             self.sqs_client.retry_attempts+1)


class TestSendMessageBatch(AsyncSQSBase):

    def setUp(self):
        super(TestSendMessageBatch, self).setUp()
        self.batch = [
            SendMessageRequestEntry(
                Id='test_msg_001',
                MessageBody='foo',
                DelaySeconds=0,
                MessageAttributes={},
            ),
            SendMessageRequestEntry(
                Id='test_msg_002',
                MessageBody='bar',
                DelaySeconds=0,
                MessageAttributes={},
            ),
        ]

    @gen_test
    def test_call(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         200,
                         self._read_data('send_message_batch.xml'))
            response = yield self.sqs_client.send_message_batch(*self.batch)
            self.assertEqual(response.Successful, self.batch)

    @gen_test
    def test_invalid_response(self):
        """Ensure all API methods raise ResponseParserError on invalid data
        """
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock, 200, 'hello')

            with self.assertRaises(ResponseParserError):
                yield self.sqs_client.send_message_batch(*self.batch)

    @gen_test
    def test_retry(self):
        with mock.patch.object(self.http_client, 'fetch') as fetch_mock:
            _setup_fetch(fetch_mock,
                         500,
                         self._read_data('sqs_error_response.xml'))
            yield self.sqs_client.send_message_batch(*self.batch,
                                                     retry=True)
            total_requests = (
                # Retry each message in the batch
                ((self.sqs_client.retry_attempts + 1) * len(self.batch)) +
                # Retry the entire operation
                self.sqs_client.retry_attempts + 1
            )
            self.assertEqual(fetch_mock.call_count, total_requests)


def all():
    suite = unittest.TestLoader().loadTestsFromName(__name__)
    return suite


if __name__ == '__main__':
    main()
