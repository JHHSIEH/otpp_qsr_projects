from unittest import TestCase
from server.trading import calc


class Test(TestCase):
    def test_test(self):
        input = 'josh'
        expected = 'JOSH'
        self.assertEqual(pnl.test(input), expected)
