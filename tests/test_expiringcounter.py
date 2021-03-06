import unittest
import warnings
from collections import Counter
from cs.eyrie.vassal import ExpiringCounter, TornadoExpiringCounter
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test, main


class TestExpiringCounter(unittest.TestCase):

    def test_init(self):
        empty_expiring_counter = ExpiringCounter()
        self.assertEqual(len(empty_expiring_counter._epochs), 1)
        primed_expiring_counter = ExpiringCounter([Counter(), Counter()])
        self.assertEqual(len(primed_expiring_counter._epochs), 2)

    def test_maxlen(self):
        expiring_counter = ExpiringCounter([Counter(), Counter(), Counter()],
                                           maxlen=2)
        self.assertEqual(expiring_counter._epochs.maxlen, 2)
        self.assertEqual(len(expiring_counter._epochs), 2)

    def test_contains_last(self):
        iterable = [Counter(), Counter(), Counter(bar=3)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertIn('bar', expiring_counter)
        self.assertNotIn('baz', expiring_counter)

    def test_contains_middle(self):
        iterable = [Counter(), Counter(bar=3), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertIn('bar', expiring_counter)
        self.assertNotIn('baz', expiring_counter)

    def test_contains_first(self):
        iterable = [Counter(bar=3), Counter(), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertIn('bar', expiring_counter)
        self.assertNotIn('baz', expiring_counter)

    def test_delitem_last(self):
        iterable = [Counter(), Counter(), Counter(bar=3)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertIn('bar', expiring_counter)
        del expiring_counter['bar']
        self.assertNotIn('bar', expiring_counter)

    def test_delitem_middle(self):
        iterable = [Counter(), Counter(bar=3), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertIn('bar', expiring_counter)
        del expiring_counter['bar']
        self.assertNotIn('bar', expiring_counter)

    def test_delitem_first(self):
        iterable = [Counter(bar=3), Counter(), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertIn('bar', expiring_counter)
        del expiring_counter['bar']
        self.assertNotIn('bar', expiring_counter)

    def test_delitem_multi(self):
        iterable = [Counter(bar=3), Counter(), Counter(bar=2)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertIn('bar', expiring_counter)
        del expiring_counter['bar']
        self.assertNotIn('bar', expiring_counter)

    def test_getitem_last(self):
        iterable = [Counter(), Counter(), Counter(bar=3)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(expiring_counter['bar'], 3)

    def test_getitem_middle(self):
        iterable = [Counter(), Counter(bar=3), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(expiring_counter['bar'], 3)

    def test_getitem_first(self):
        iterable = [Counter(bar=3), Counter(), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(expiring_counter['bar'], 3)

    def test_getitem_multi(self):
        iterable = [Counter(bar=3), Counter(), Counter(bar=2)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(expiring_counter['bar'], 5)

    def test_increment(self):
        iterable = [Counter(), Counter(), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        expiring_counter['foo'] = 1
        self.assertEqual(expiring_counter['foo'], 1)
        expiring_counter.tick()

        expiring_counter.increment('foo', 1)
        self.assertEqual(expiring_counter['foo'], 2)
        expiring_counter.tick()

        expiring_counter.increment('foo', 1)
        self.assertEqual(expiring_counter['foo'], 3)
        expiring_counter.tick()
        self.assertEqual(expiring_counter['foo'], 2)

    def test_iter_last(self):
        iterable = [Counter(), Counter(), Counter(bar=3)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual([key for key in expiring_counter], ['bar'])

    def test_iter_middle(self):
        iterable = [Counter(), Counter(bar=3), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual([key for key in expiring_counter], ['bar'])

    def test_iter_first(self):
        iterable = [Counter(bar=3), Counter(), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual([key for key in expiring_counter], ['bar'])

    def test_iter_multi(self):
        iterable = [Counter(bar=3), Counter(), Counter(bar=4)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual([key for key in expiring_counter],
                         ['bar'])

    def test_iter_multi_distinct(self):
        iterable = [Counter(bar=3), Counter(baz=2), Counter(bing=4)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual([key for key in expiring_counter],
                         ['bar', 'baz', 'bing'])

    def test_len_last(self):
        iterable = [Counter(), Counter(), Counter(bar=3)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(len(expiring_counter), 1)

    def test_len_middle(self):
        iterable = [Counter(), Counter(bar=3), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(len(expiring_counter), 1)

    def test_len_first(self):
        iterable = [Counter(bar=3), Counter(), Counter()]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(len(expiring_counter), 1)

    def test_len_multi(self):
        iterable = [Counter(bar=3), Counter(), Counter(bar=2)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(len(expiring_counter), 2)

    def test_len_multi_distinct(self):
        iterable = [Counter(bar=3), Counter(), Counter(baz=2)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(len(expiring_counter), 2)

    def test_setitem(self):
        expiring_counter = ExpiringCounter(maxlen=3)
        expiring_counter['foo'] += 1
        self.assertEqual(expiring_counter['foo'], 1)

    def test_setitem_warning(self):
        expiring_counter = ExpiringCounter(maxlen=3)
        expiring_counter['bar'] = 1
        try:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            with warnings.catch_warnings(record=True) as result:
                # This should trigger a warning because Python
                # does a __getitem__ call, increment, then __setitem__
                # This sequence breaks if one of the elements is in an
                # earlier epoch
                expiring_counter['bar'] += 1
                # Verify some things
                self.assertEqual(len(result), 1)
                self.assertIs(result[-1].category, SyntaxWarning)
                self.assertIn("increment", str(result[-1].message))
        finally:
            warnings.resetwarnings()

    def test_clear(self):
        expiring_counter = ExpiringCounter(maxlen=3)
        expiring_counter['foo'] += 1
        self.assertEqual(expiring_counter['foo'], 1)
        expiring_counter.clear()
        self.assertEqual(expiring_counter['foo'], 0)

    def test_tick(self):
        iterable = [Counter(), Counter(), Counter(bar=3)]
        expiring_counter = ExpiringCounter(iterable, maxlen=3)
        self.assertEqual(expiring_counter['bar'], 3)
        self.assertEqual(len(expiring_counter), 1)
        expiring_counter.tick()
        self.assertEqual(expiring_counter['bar'], 3)
        self.assertEqual(len(expiring_counter), 1)
        expiring_counter.tick()
        self.assertEqual(expiring_counter['bar'], 3)
        self.assertEqual(len(expiring_counter), 1)
        expiring_counter.tick()
        self.assertEqual(expiring_counter['bar'], 0)
        self.assertEqual(len(expiring_counter), 0)


class TestTornadoExpiringCounter(AsyncTestCase):

    @gen_test
    def test_init(self):
        expiring_counter = TornadoExpiringCounter(self.io_loop,
                                                  granularity=None,
                                                  maxlen=3)
        self.assertEqual(expiring_counter._epochs.maxlen, 3)
        self.assertIs(expiring_counter._tick_pc, None)
        expiring_counter = TornadoExpiringCounter(self.io_loop,
                                                  granularity=gen.moment,
                                                  maxlen=3)
        self.assertIs(expiring_counter._tick_pc, None)
        expiring_counter = TornadoExpiringCounter(self.io_loop,
                                                  granularity=0.0,
                                                  maxlen=3)
        self.assertIs(expiring_counter._tick_pc, None)

    @gen_test
    def test_tick(self):
        expiring_counter = TornadoExpiringCounter(self.io_loop,
                                                  granularity=None,
                                                  maxlen=1)
        expiring_counter['foo'] += 1
        self.assertEqual(expiring_counter['foo'], 1)
        self.assertEqual(len(expiring_counter), 1)
        # Let IOLoop advance one iteration
        yield None
        self.assertEqual(expiring_counter['foo'], 0)
        self.assertEqual(len(expiring_counter), 0)
        expiring_counter = TornadoExpiringCounter(self.io_loop,
                                                  granularity=None,
                                                  maxlen=2)
        expiring_counter['foo'] += 1
        self.assertEqual(expiring_counter['foo'], 1)
        self.assertEqual(len(expiring_counter), 1)
        # Let IOLoop advance one iteration
        yield None
        self.assertEqual(expiring_counter['foo'], 1)
        self.assertEqual(len(expiring_counter), 1)
        yield None
        self.assertEqual(expiring_counter['foo'], 0)
        self.assertEqual(expiring_counter['foo'], 0)


def all():
    suite = unittest.TestLoader().loadTestsFromName(__name__)
    return suite


if __name__ == '__main__':
    main()
