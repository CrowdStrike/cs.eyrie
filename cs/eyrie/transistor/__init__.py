"""
There are 2 primary purposes of this module:
    1. Provide back-pressure so that memory is conserved when downstream
       services slow down
    2. Provide a unified interface for swapping in source/draining services

It is possible to pair a Kafka consumer with a ZMQ sender, or vice-versa, pair
a ZMQ receiver with a Kafka producer. All communication is async, using Tornado
queues throughout.
"""
RUNNING, CLOSING, CLOSED = range(3)
DEFAULT_TRANSDUCER_CONCURRENCY = 1
try:
    from confluent_kafka import KafkaError
except ImportError:
    TRANSIENT_ERRORS = set()
else:
    TRANSIENT_ERRORS = set([KafkaError._ALL_BROKERS_DOWN, KafkaError._TRANSPORT])
from cs.eyrie.transistor.drain import (
    QueueDrain,
    RDKafkaDrain,
    RoutingDrain,
    RoutingMessage,
    StreamDrain,
    SQSDrain,
    ZMQDrain,
)
from cs.eyrie.transistor.gate import (
    BufferedGate,
    Gate,
    Transistor,
)
from cs.eyrie.transistor.source import (
    KafkaMessage,
    PailfileSource,
    QueueSource,
    RDKafkaSource,
    SQSSource,
    StreamSource,
    ZMQSource,
)
from cs.eyrie.transistor.sqs import (
    AsyncSQSClient,
    SendMessageRequestEntry,
    SQSError,
    build_send_message_request,
    deserialize_send_message_request,
    serialize_send_message_request,
)


def get_last_element(msg):
    if isinstance(msg, (list, tuple)):
        return msg[-1]
    return msg
