# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
from __future__ import absolute_import

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO
from collections import Counter, OrderedDict, defaultdict, deque, namedtuple
from datetime import timedelta
from functools import partial
import logging
import multiprocessing
import os
import sys
from uuid import UUID

try:
    from dateutil.parser import parse as dt_parse
except ImportError:
    dt_parse = None

try:
    from sixfeetup.bowab.db import init_sa
except ImportError:
    init_sa = None

try:
    from psycopg2 import Error
    from psycopg2.extras import register_uuid
    from psycopg2.extras import DictCursor
except ImportError:
    Error = None
    register_uuid = None
    DictCursor = None

from pyramid.config import Configurator
from pyramid.paster import get_appsettings

from tornado.concurrent import is_future

try:
    import unicodecsv as csv
except ImportError:
    import csv

import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

from cs.eyrie.config import SOCKET_TYPES
from cs.eyrie.config import ZMQChannel


csv.field_size_limit(sys.maxint)


class Vassal(object):
    channels = {
        'control': ZMQChannel(
            endpoint='ipc:///tmp/eyrie_herald',
            socket_type=zmq.SUB,
        ),
    }
    title = '(eyrie:vassal)'
    app_name = 'eyrie'
    args = None
    cursor_factory = DictCursor

    def __init__(self, **kwargs):
        self.pks_seen = defaultdict(set)
        self.curr_proc = multiprocessing.current_process()

        # Set up logging. By default, instances of this class will use a
        # logger with a name derived from the class & module names.
        # Optionally, the -l/--log-handler argument can be provided on the
        # command line to override this default.
        logger_name = '.'.join([self.__class__.__module__,
                                self.__class__.__name__])
        log_handler = kwargs.get('log-handler') or logger_name
        self.logger = logging.getLogger(log_handler)

        self.context = kwargs.get('context', zmq.Context())
        loop = kwargs.pop('loop', None)
        self.set_ioloop(loop)

        self.config_uri = kwargs.pop('config')

        if kwargs.get('init_streams', True):
            self.init_streams()

        app_settings = get_appsettings(self.config_uri, name=self.app_name)
        self.config = Configurator(settings=app_settings)
        self.curr_proc.authkey = self.config.registry.settings['eyrie.authkey'].encode('utf8')

        if kwargs.get('init_db', False):
            self.init_db()

    def set_ioloop(self, loop=None):
        if loop is None:
            ioloop.install()
            self.loop = ioloop.IOLoop.instance()
        else:
            self.loop = loop

    def init_db(self):
        if init_sa is None:
            self.logger.error("Database support requires cs.eyrie to be installed with the PostgreSQL extra: install_requires = ['cs.eyrie[PostgreSQL]']")
            self.terminate()
        else:
            register_uuid()
            # TODO: look into using Momoko for async
            #       processing using Tornado's IOLoop
            #       (LISTEN/NOTIFY currently not supported)
            #       https://github.com/FSX/momoko/issues/32
            self.db_session = init_sa(self.config, application_name=self.title)
            self.db_engine = self.db_session.get_bind()
            self.db_conn = self.db_engine.raw_connection()
            # Ensure we back out of any automatic transaction SQLAlchemy started
            self.db_conn.rollback()
            self.db_conn.set_session(autocommit=True)

            self.cursor = self.db_conn.cursor(cursor_factory=self.cursor_factory)
            self.cursor.arraysize = 1024

    def init_streams(self):
        self.counters = Counter()
        self.streams = {}
        for cname, channel in self.channels.items():
            self.connect_stream(cname, channel)

    def connect_stream(self, sname, zmq_channel):
        socket = self.context.socket(zmq_channel.socket_type)

        if zmq_channel.bind:
            socket.bind(zmq_channel.endpoint)
            if zmq_channel.socket_type == zmq.SUB:
                # Workaround for https://zeromq.jira.com/browse/LIBZMQ-270
                socket.getsockopt(zmq.EVENTS)
        else:
            socket.connect(zmq_channel.endpoint)

        if zmq_channel.socket_type == zmq.SUB and zmq_channel.subscription:
            for prefix in zmq_channel.subscription:
                socket.setsockopt(zmq.SUBSCRIBE, prefix.encode('utf8'))

        stream = ZMQStream(socket, io_loop=self.loop)
        stream.channel_name = sname

        # Wire up any defined handlers
        self.init_recv(sname, stream)

        tname = SOCKET_TYPES[zmq_channel.socket_type]
        ctype = 'Bound' if zmq_channel.bind else 'Connected'
        self.logger.debug("%s %s stream to address %s",
                          ctype, tname, zmq_channel.endpoint)
        self.streams[sname] = stream

    def get_recv_handler(self, cname):
        hname = self.channels[cname].recv_handler
        if hname:
            if callable(hname):
                return hname
            else:
                return getattr(self, hname)
        else:
            hname = 'on{}'.format(cname.replace('_', ' ').title().replace(' ', ''))

        if hasattr(self, hname):
            return getattr(self, hname)
        else:
            return None

    def init_recv(self, sname, stream):
        handler = self.get_recv_handler(sname)
        if handler:
            stream.on_recv_stream(self.onRecvStream)

        if self.channels[sname].drains is not None:
            stream.on_send_stream(self.onSendStream)
            cb = lambda msg, status: self.onSendStream(stream, msg, status)
            stream.send_multipart = partial(stream.send_multipart, callback=cb)

    def terminate(self):
        self.logger.info('%s is terminating', self.__class__.__name__)
        for sname, stream in self.streams.items():
            self.logger.debug('Flushing stream: %s', sname)
            num_flushed = stream.flush()
            if num_flushed:
                self.logger.debug('Flushed %d messages', num_flushed)

        logging.shutdown()
        self.loop.stop()

    def onCommand(self, cmd):
        if cmd[1] == 'TERM':
            msg = "Received exit command, %s will stop receiving messages"
            self.logger.warning(msg, self.__class__.__name__)
            self.terminate()

    def onFuture(self, fut):
        try:
            fut.result()
        except Exception as err:
            self.logger.exception(err)

    def onRecvStream(self, stream, msg):
        # If the handler triggers an exception, pyzmq will disable it
        # Here we catch any exception and just log it, so that processing
        # can continue
        try:
            cname = stream.channel_name
            self.counters[cname] += 1
            #buf_len = self.counters[cname] - self.counters[output_cname]
            #self.logger.debug('Received on %s: %d', cname, buf_len)
            handler = self.get_recv_handler(cname)
            fut = handler(msg)
            if is_future(fut):
                self.loop.add_future(fut, self.onFuture)

            output_cname = self.channels[cname].drained_by
            if output_cname:
                buf_len = self.streams[output_cname]._send_queue.qsize()
                hwm = self.channels[cname].hwm
                if buf_len >= (hwm - (hwm * 0.2)):
                    msg = "Pausing receive on: %s (Buffered: %d, HWM: %d)"
                    hwm = self.channels[cname].hwm
                    self.logger.info(msg, cname, buf_len, hwm)
                    stream.stop_on_recv()
        except Error as err:
            self.logger.exception(err)
            self.init_db()
        except Exception as err:
            self.logger.exception(err)

    def onSendStream(self, stream, msg, status):
        # If the handler triggers an exception, pyzmq will disable it
        # Here we catch any exception and just log it, so that processing
        # can continue
        try:
            cname = stream.channel_name
            self.counters[cname] += 1
            input_cname = self.channels[cname].drains
            #buf_len = self.counters[input_cname] - self.counters[cname]
            #self.logger.debug('Sent on %s: %d', cname, buf_len)
            if input_cname and not self.streams[input_cname].receiving():
                buf_len = stream._send_queue.qsize()
                hwm = self.channels[input_cname].hwm
                if buf_len <= self.channels[input_cname].hwm:
                    handler = self.get_recv_handler(input_cname)
                    if handler:
                        txt = "Resuming receive on: %s (Buffered: %d, HWM: %d)"
                        self.logger.info(txt, input_cname, buf_len, hwm)
                        input_stream = self.streams[input_cname]
                        input_stream.on_recv_stream(self.onRecvStream)
                #else:
                #    txt = "Still paused on: %s (Buffered: %d, HWM: %d)"
                #    self.logger.debug(txt, input_cname, buf_len, hwm)
        except Exception as err:
            self.logger.exception(err)


_ValidationOp = namedtuple("_ValidationOp", ["c_name", "v_op", "c_type"])


class _TableRowValidator():

    type_checks = {
        'BIGINT': int,
        'BIGINTEGER': int,
        'DECIMAL': float,
        'FLOAT': float,
        'INT': int,
        'INTEGER': int,
        'DATE': dt_parse,
        'DATETIME': dt_parse,
        'REAL': float,
        'SMALLINT': int,
        'SMALLINTEGER': int,
        'TIME': dt_parse,
        'TIMESTAMP': dt_parse,
        'UUID': UUID,
    }

    def __init__(self, table):
        """ table is of type with iter(columns) name,type """
        self.validation_ops = []
        for column in table.columns:
            c_type = str(column.type).upper()
            v_op = self.type_checks.get(c_type, lambda x: True)
            self.validation_ops.append(
                _ValidationOp(column.name, v_op, c_type)
            )

    def validate_row(self, row):
        """ Returns list of validation errors """
        errors = []
        for v in self.validation_ops:
            data = row[v.c_name]
            try:
                v.v_op(data)
            except Exception:
                msg = 'Invalid data for type {} column "{}": {}'
                errors.append(msg.format(v.c_type, v.c_name, data))
        return errors


class BatchVassal(Vassal):
    exclude_cols = []
    delay = 15
    models = []

    def __init__(self, **kwargs):
        super(BatchVassal, self).__init__(**kwargs)
        self.tables = []
        for m in self.models:
            id_table = m.__table__.info.get('id_table', None)
            if id_table is not None:
                self.tables.append(id_table)
            self.tables.append(m.__table__)
        self.tables_by_name = {t.fullname: t for t in self.tables}
        self.batch = deque()
        self.row_counts = Counter()
        self.init_writers()
        self.add_batch_timeout()
        self.row_validators = { t.fullname: _TableRowValidator(t)
            for t in self.tables }

    def add_batch_timeout(self):
        if self.delay is None:
            self.loop.add_callback(self.send_batch)
        else:
            self.loop.add_timeout(timedelta(seconds=self.delay),
                                  self.send_batch)

    def init_writers(self):
        self.bufs = {}
        self.writers = {}
        for tname in self.tables_by_name:
            self.init_writer(tname)

    def init_writer(self, name):
        self.bufs[name] = StringIO()
        table = self.tables_by_name[name]
        columns = [
            i.name
            for i in table.columns
            if i.name not in self.exclude_cols
        ]
        self.writers[name] = csv.DictWriter(self.bufs[name], columns,
                                            extrasaction='ignore',
                                            delimiter=',')

    def init_reader(self, name, cols=None):
        buf = self.bufs[name]
        table = self.tables_by_name[name]
        if cols is None:
            cols = [
                i.name
                for i in table.columns
                if i.name not in self.exclude_cols
            ]
        buf.seek(0)
        return csv.DictReader(buf, cols, delimiter=',')

    def copy_from(self, name, savepoint=True):
        cols = ', '.join([
            i.name
            for i in self.tables_by_name[name].columns
            if i.name not in self.exclude_cols
        ])
        sql = 'COPY %s (%s) FROM STDIN WITH CSV'
        sql = sql % (name, cols)
        self.logger.debug("Beginning COPY for %s: %d",
                          name, self.row_counts[name])

        sname = name.replace('.', '_')
        if savepoint:
            # NOTE: we can't use parameters, as they will be quoted as
            #       data (i.e., using ' ').
            self.cursor.execute('SAVEPOINT {};'.format(sname))

        self.bufs[name].seek(0)
        self.delete_from(name)
        self.bufs[name].seek(0)
        self.cursor.copy_expert(sql, self.bufs[name])

    def delete_from(self, name):
        table = self.tables_by_name[name]
        pk_cols = OrderedDict([
            (c.name, str(c.type))
            for c in table.primary_key.columns
        ])
        reader = self.init_reader(name, table.columns.keys())
        # TODO: refactor this to use SQLAlchemy's SQL expression API
        #       http://docs.sqlalchemy.org/en/rel_0_9/core/tutorial.html
        sql = ['DELETE FROM {}'.format(table.fullname)]
        sql.append('WHERE')
        where = []
        csv_keys = {pk: [] for pk in pk_cols}

        # Generate primary key restriction
        if len(pk_cols) == 1:
            where.append("\t%s = ANY(%%s)" % pk_cols.keys()[0])
        elif len(pk_cols) == 2:
            cols = ', '.join(pk_cols)
            col_def = ', '.join([' '.join(i) for i in pk_cols.items()])
            # FIXME: get rid of pair_array and inline the SQL function
            where.append("""
                (%s) IN (
                    SELECT %s
                    FROM eyrie.pair_array(%%s::%s[], %%s::%s[]) pa (%s)
                )
                """ % (cols, cols,
                       pk_cols.values()[0], pk_cols.values()[1],
                       col_def)
            )

        # Include partition control column for efficiency
        if 'partition_control' in table.info:
            pc = table.info['partition_control']
            csv_keys[pc] = []
            where.append("%s BETWEEN %%s AND %%s" % pc)

        # We special-case detect_tags, as added_by isn't part of the
        # primary key. We want to be sure to only delete tags that
        # would have been generated by us.
        if 'extra_delete_where' in table.info:
            where.append(table.info['extra_delete_where'])

        sql.append(' AND\n\t'.join(where))

        for row in reader:
            for k, vals in csv_keys.items():
                if str(table.columns.get(k).type) == 'UUID':
                    v = UUID(row[k])
                else:
                    v = row[k]
                vals.append(v)

        # Collate accumulated params
        params = []
        for pk in pk_cols:
            params.append(csv_keys[pk])
        if 'partition_control' in table.info:
            params.append(min(csv_keys[pc]))
            params.append(max(csv_keys[pc]))

        self.cursor.execute('\n'.join(sql), params)

    def send_batch(self, add_timeout=True):
        try:
            all_rows = sum(self.row_counts.values())
            if not self.batch or not all_rows:
                msg = "No data to send, waiting another %d seconds"
                self.logger.debug(msg, self.delay)
                return

            self.logger.info("Beginning batch send: %d", all_rows)
            # TODO: add logic for requeuing event data by checking for
            #       a key in the JSON.
            #       If present, nuke system-generated tags for this detect
            self.cursor.execute('BEGIN;')
            for table in self.tables:
                name = table.fullname
                self.copy_from(name)
                #self.cursor.execute('RELEASE SAVEPOINT {};'.format(name))
                self.logger.debug("COPY Finished: %s", name)
            self.cursor.execute('COMMIT;')
            all_rows = sum(self.row_counts.values())
            self.row_counts.clear()
            # All buffers were copied; it's safe now to truncate
            for buf in self.bufs.values():
                buf.seek(0)
                buf.truncate()
            self.pks_seen.clear()
            self.logger.info("Batch send complete: %d", all_rows)
        except Exception as err:
            self.logger.exception(err)
            self.cursor.execute('ROLLBACK;')
            # Reset positions on all buffers so that we can continue
            # to accumulate message data
            for buf in self.bufs.values():
                buf.seek(0, os.SEEK_END)
            raise err
        finally:
            if add_timeout:
                self.add_batch_timeout()


    def validate_row(self, name, row):
        errors = self.row_validators[name].validate_row(row)
        if errors:
            raise errors[0]


    def write_row(self, name, row):
        id_table = self.tables_by_name[name].info.get('id_table', None)
        if id_table is not None:
            self.write_row(id_table.fullname, row)
        table = self.tables_by_name[name]
        pk_cols = OrderedDict([
            (c.name, str(c.type))
            for c in table.primary_key.columns
        ])
        pk_vals = tuple([row[v] for v in pk_cols])
        if pk_vals in self.pks_seen[name]:
            self.logger.debug('Already written to %s: %s',
                              name, pk_vals)
            return
        else:
            self.pks_seen[name].add(pk_vals)
        self.writers[name].writerow(row)
        self.row_counts[name] += 1

    def write_rows(self, name, rows):
        id_table = self.tables_by_name[name].info.get('id_table', None)
        if id_table is not None:
            self.write_rows(id_table.fullname, rows)
        self.writers[name].writerows(rows)
        self.row_counts[name] += len(rows)
