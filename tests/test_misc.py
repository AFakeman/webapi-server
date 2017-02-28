import unittest

from webapi_server.server import random_string

class TestRandStr(unittest.TestCase):
    def test_length(self):
        self.assertEqual(len(random_string(['1', '2'], 16)), 16)

    def test_alphabet(self):
        alphabet = ['1', '2']
        result = random_string(alphabet, 16)
        real_alphabet = set([i for i in result])
        self.assertTrue(real_alphabet.issubset(set(alphabet)))
