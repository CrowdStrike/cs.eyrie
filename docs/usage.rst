========
Usage
========

The primary purpose of this package is to provide a base class for long-running daemons that make use of multiple ZMQ sockets. Here is a complete example.

First, imports and port definitions:

.. literalinclude:: code/my_zmq.py
    :lines: 1-10

Now the server daemon (note the declarative config of the ZMQ sockets):

.. literalinclude:: code/my_zmq.py
    :lines: 13-52
    :emphasize-lines: 4-13

Next is the worker daemon (note that by convention the handler called is named after the channel; you may use `recv_handler` in the config to change that):

.. literalinclude:: code/my_zmq.py
    :lines: 55-76
    :emphasize-lines: 16,19

Now to run them both, with a single server daemon and 4 workers:

.. literalinclude:: code/my_zmq.py
    :lines: 79-96
    :emphasize-lines: 80-85

And the INI file:

.. literalinclude:: code/my_zmq.ini
   :language: ini

And finally, to run::

    python <path/to/>my_zmq.py -c <path/to/>my_zmq.ini

This results in the following output::

    (my:worker:3) MyWorker has begun processing messages
    (my:worker:1) MyWorker has begun processing messages
    (my:worker:4) MyWorker has begun processing messages
    (my:server)   MyServer has begun processing messages
    (my:worker:2) MyWorker has begun processing messages
    (my:server)   Sending job: This is message #1
    (my:worker:2) Received job: ['This is message #1']
    (my:server)   Broadcasting: This is message #2
    (my:worker:2) Received broadcast: ['This is message #2']
    (my:worker:4) Received broadcast: ['This is message #2']
    (my:worker:3) Received broadcast: ['This is message #2']
    (my:worker:1) Received broadcast: ['This is message #2']
    (my:server)   Sending job: This is message #3
    (my:worker:3) Received job: ['This is message #3']
    (my:server)   Broadcasting: This is message #4
    (my:worker:4) Received broadcast: ['This is message #4']
    (my:worker:2) Received broadcast: ['This is message #4']
    (my:worker:3) Received broadcast: ['This is message #4']
    (my:worker:1) Received broadcast: ['This is message #4']
    (my:server)   Sending job: This is message #5
    (my:server)   Broadcasting: EXIT
    (my:server)   MyServer is terminating
    (my:worker:1) Received job: ['This is message #5']
    (my:worker:1) Received broadcast: ['EXIT']
    (my:worker:2) Received broadcast: ['EXIT']
    (my:worker:4) Received broadcast: ['EXIT']
    (my:worker:1) MyWorker is terminating
    (my:worker:4) MyWorker is terminating
    (my:worker:2) MyWorker is terminating
    (my:worker:3) Received broadcast: ['EXIT']
    (my:worker:3) MyWorker is terminating

This sample illustrates the difference between ZMQ's PUSH/PULL and PUB/SUB.
Messages in PUSH/PULL are queued round-robin through all connected workers.
Messages in PUB/SUB are broadcast to all connected workers (as long as their subscription filter matches).
