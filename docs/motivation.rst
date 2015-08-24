===============================
Motivation
===============================

This package is a collection of abstractions to make building applications
with ZeroMQ easier (both to develop and debug).
The Counter-Intrustion and Detections Labs (CID Labs) team at CrowdStrike
is responsible for fielding many different types of analysis systems,
but we invariably use ZMQ as the glue layer between each.

The Python ZMQ bindings are excellent, in that they provide access to the
underlying libzmq C++ API. However, because they wrap the lowest-level
abstractions it can be difficult to get started. For example, there are 3
methods for interacting with ZMQ sockets: waiting synchronously for data,
polling or using the Tornado IOLoop. This package advocates for this last
method, since it allows the developer to concentrate on what they want to
accomplish, rather than the details of how it is done. It also extends this
facility by taking care of the boilerplate of socket definition and wiring
of event handlers.

To demonstrate, here is a series of examples to show the differences:

.. code-block:: python

    import zmq
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.SUBSCRIBE, "")
    socket.connect("tcp://localhost:9090")
    while True:
        msg = socket.recv()

In this example, our program is limited to the single loop, and receiving
more messages waits until we've processed each message.


The following example uses the Poller object to allow consuming from
multiple sockets. However, it also becomes much more complex. The boilerplate
code for handling the socket events is mixed in with application logic,
and it is much less clear what the developer was intending to do.

.. code-block:: python

    import zmq
    context = zmq.Context()

    socket_pull = context.socket(zmq.PULL)
    socket_pull.connect ("tcp://localhost:9090")
    socket_sub = context.socket(zmq.SUB)
    socket_sub.connect ("tcp://localhost:9095")
    socket_sub.setsockopt(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(socket_pull, zmq.POLLIN)
    poller.register(socket_sub, zmq.POLLIN)

    should_continue = True
    while should_continue:
        socks = dict(poller.poll())
        if socket_pull in socks and socks[socket_pull] == zmq.POLLIN:
            pull_msg = socket_pull.recv()
            if pull_msg == "Exit":
                should_continue = False

        if socket_sub in socks and socks[socket_sub] == zmq.POLLIN:
            sub_msg = socket_sub.recv()


The third example uses the Tornado IOLoop to abstract the details of
the socket communication, but you still have imperative code to setup
the sockets in the first place:


.. code-block:: python

    from zmq.eventloop import ioloop, zmqstream

    def process_pull(msg):
        if msg[0] == "Exit":
            print "Received exit command, client will stop receiving messages"
            should_continue = False
            ioloop.IOLoop.instance().stop()

    def process_sub(msg):
        print "Processing ... %s" % msg

    context = zmq.Context()
    socket_pull = context.socket(zmq.PULL)
    socket_pull.connect ("tcp://localhost:9090")
    stream_pull = zmqstream.ZMQStream(socket_pull)
    stream_pull.on_recv(getcommand)

    socket_sub = context.socket(zmq.SUB)
    socket_sub.connect ("tcp://localhost:9095")
    socket_sub.setsockopt(zmq.SUBSCRIBE, "")
    stream_sub = zmqstream.ZMQStream(socket_sub)
    stream_sub.on_recv(process_message)


The final example uses the abstractions provided by this package to make
it declaritively configure the ZMQ sockets you want, but gets out of your
way after that.


.. code-block:: python

    from cs.eyrie import Vassal, ZMQChannel

    class MyWorker(Vassal):
        channels = dict(
            Vassal.channels,
            pull=ZMQChannel(
                endpoint="tcp://localhost:9090",
                socket_type=zmq.PULL,
            ),
            sub=ZMQChannel(
                endpoint="tcp://localhost:9095",
                socket_type=zmq.SUB,
                subscription=[''],
            ),
        )

        def onPull(self, msg):
            print "Received exit command, client will stop receiving messages"

        def onSub(self, msg):
            print "Processing ... %s" % msg


Here you see that config is abstracted out of the program flow, and you only need
to implement the event handlers that are called for each channel definition.
