"""
Tests for internal row validator
"""
import unittest

from cs.eyrie import vassal
from collections import namedtuple


Column = namedtuple('Column', ['name', 'type', 'nullable'])


class _FakeTable():

    def __init__(self, columns):
        self.columns = columns


class TestRowValidator(unittest.TestCase):

    def setUp(self):
        self.fake_table = _FakeTable([
            Column('BIGINT', 'BIGINT', True),
            Column('BIGINTEGER', 'BIGINTEGER', True),
            Column('DECIMAL', 'DECIMAL', True),
            Column('FLOAT', 'FLOAT', True),
            Column('INT', 'INT', True),
            Column('INTEGER', 'INTEGER', True),
            Column('DATE', 'DATE', True),
            Column('DATETIME', 'DATETIME', True),
            Column('REAL', 'REAL', True),
            Column('SMALLINT', 'SMALLINT', True),
            Column('SMALLINTEGER', 'SMALLINTEGER', True),
            Column('TIME', 'TIME', True),
            Column('TIMESTAMP', 'TIMESTAMP', True),
            Column('UUID', 'UUID', True),
            Column('RequiredUUID', 'UUID', False),
        ])
        self.table_validator = vassal._TableRowValidator(self.fake_table)

    def build_row(self, **kwargs):
        row = {
            'BIGINT': '12455555555',
            'BIGINTEGER': '1151521521',
            'DECIMAL': '234234.32423432',
            'FLOAT': '2535233.253253',
            'INT': '253235',
            'INTEGER': '253325',
            'DATE': '1982-12-31',
            'DATETIME': '1982-12-31T23:00:00Z',
            'REAL': '125125.325',
            'SMALLINT': '234',
            'SMALLINTEGER': '125421',
            'TIME': '23:00:00Z',
            'TIMESTAMP': '1982-12-31T23:00:00Z',
            'UUID': '123e4567-e89b-12d3-a456-426655440000',
            'RequiredUUID': '123e4567-e89b-12d3-a456-426655440000',
        }
        row.update(kwargs)
        return row

    def test_all_valid(self):
        errors = self.table_validator.validate_row(self.build_row())
        self.assertEqual(len(errors),
                         0,
                         'Unexpected errors: {}'.format(errors))

    def test_invalid_timestamp(self):
        # These values observed from PSO's Reaper
        errors = self.table_validator.validate_row(self.build_row(
            DATETIME='+20162016-11-01T02:00:00',
            TIMESTAMP='181816-10-28T06:40:00',
        ))
        self.assertEqual(len(errors),
                         2,
                         'Wrong number of errors: {}'.format(errors))

    def test_invalid(self):
        for column in self.fake_table.columns:
            kwargs = {column.name: 'foobar'}
            errors = self.table_validator.validate_row(self.build_row(**kwargs))
            self.assertEqual(len(errors),
                             1,
                             'Wrong number of errors: {}'.format(errors))

    def test_nullable(self):
        errors = self.table_validator.validate_row(self.build_row(
            RequiredUUID='',
        ))
        self.assertEqual(len(errors),
                         1,
                         'Wrong number of errors: {}'.format(errors))

    def tearDown(self):
        pass
