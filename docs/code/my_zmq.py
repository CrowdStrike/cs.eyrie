from multiprocessing import Process

from tornado.ioloop import PeriodicCallback

import zmq

from cs.eyrie import Vassal, ZMQChannel, script_main

CMD_PORT = 9090
BROADCAST_PORT = 9095


class MyServer(Vassal):
    app_name = 'my_zmq'
    channels = dict(
        job=ZMQChannel(
            endpoint="tcp://*:{}".format(CMD_PORT),
            socket_type=zmq.PUSH,
            bind=True,
        ),
        broadcast=ZMQChannel(
            endpoint="tcp://*:{}".format(BROADCAST_PORT),
            socket_type=zmq.PUB,
            bind=True,
        ),
    )
    title = '(my:server)'
    messages = 0

    def __init__(self, **kwargs):
        super(MyServer, self).__init__(**kwargs)
        pc = PeriodicCallback(self.execute, 2 * 1000, self.loop)
        pc.start()

    def execute(self):
        self.messages += 1
        msg = 'This is message #{}'.format(self.messages)
        if self.messages % 2:
            self.send_job(msg)
        else:
            self.broadcast(msg)
        if self.messages >= 5:
            self.loop.add_callback(self.broadcast, 'EXIT')
            self.loop.add_callback(self.terminate)

    def send_job(self, msg):
        self.logger.info("Sending job: %s", msg)
        self.streams['job'].send(msg)

    def broadcast(self, msg):
        self.logger.info("Broadcasting: %s", msg)
        self.streams['broadcast'].send(msg)


class MyWorker(Vassal):
    app_name = 'my_zmq'
    channels = dict(
        job=ZMQChannel(
            endpoint="tcp://localhost:{}".format(CMD_PORT),
            socket_type=zmq.PULL,
        ),
        broadcast=ZMQChannel(
            endpoint="tcp://localhost:{}".format(BROADCAST_PORT),
            socket_type=zmq.SUB,
            subscription=[''],
        ),
    )
    title = '(my:worker)'

    def onJob(self, msg):
        self.logger.info("Received job: %s", msg)

    def onBroadcast(self, msg):
        self.logger.info("Received broadcast: %s", msg)
        if msg[0] == 'EXIT':
            self.loop.add_callback(self.terminate)


def main():
    workers = [
        Process(
            target=script_main,
            args=(MyWorker, None),
            kwargs=dict(title='(my:worker:{})'.format(worker+1)),
        )
        for worker in range(4)
    ]
    for worker in workers:
        worker.start()
    my_server = Process(target=script_main,
                        args=(MyServer, None))
    my_server.start()
    my_server.join()
    for worker in workers:
        worker.join()


if __name__ == "__main__":
    main()
