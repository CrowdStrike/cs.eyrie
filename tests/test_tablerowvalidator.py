"""
Tests for internal row validator
"""

import unittest

from cs.eyrie import vassal
from collections import namedtuple


Column = namedtuple('Column', ['name', 'type']
class _FakeTable():
    __init__(columns):
        self.columns = columns


class TestRowValidator(unittest.TestCase):
    def setUp(self):
        self.fake_table = _FakeTable([
                Column('BIGINT','BIGINT'),
                Column('BIGINTEGER','BIGINTEGER'),
                Column('DECIMAL','DECIMAL'),
                Column('FLOAT','FLOAT'),
                Column('INT','INT'),
                Column('INTEGER','INTEGER'),
                Column('DATE','DATE'),
                Column('DATETIME','DATETIME'),
                Column('REAL','REAL'),
                Column('SMALLINT','SMALLINT'),
                Column('SMALLINTEGER','SMALLINTEGER'),
                Column('TIME','TIME'),
                Column('TIMESTAMP','TIMESTAMP'),
                Column('UUID','UUID'),
            ])
        self.table_validator = vassal._TableRowValidator(fake_table)

    def all_valid(self):
        row = {
                'BIGINT':'12455555555',
                'BIGINTEGER':'1151521521',
                'DECIMAL':'234234.32423432',
                'FLOAT':'2535233.253253',
                'INT':'253235',
                'INTEGER':'253325',
                'DATE':'1982-12-31',
                'DATETIME':'1982-12-31T23:00:00Z',
                'REAL':'125125.325',
                'SMALLINT':'234',
                'SMALLINTEGER':'125421',
                'TIME':'23:00:00Z',
                'TIMESTAMP':'1982-12-31T23:00:00Z',
                'UUID':'123e4567-e89b-12d3-a456-426655440000'
        }
        errors = self.table_validator.validate_row(row)
        unittest.assertEqual(len(errors), 0, 'No errors')

    def invalid_date(self):
        row = {
                'BIGINT':'12455555555',
                'BIGINTEGER':'1151521521',
                'DECIMAL':'234234.32423432',
                'FLOAT':'2535233.253253',
                'INT':'253235',
                'INTEGER':'253325',
                'DATE':'foobar',
                'DATETIME':'foobar',
                'REAL':'125125.325',
                'SMALLINT':'234',
                'SMALLINTEGER':'125421',
                'TIME':'foobar',
                'TIMESTAMP':'foobar',
                'UUID':'123e4567-e89b-12d3-a456-426655440000'
        }
        errors = self.table_validator.validate_row(row)
        unittest.assertEqual(len(errors), 4, 'Four dates fail')

    def tearDown(self):
        pass

