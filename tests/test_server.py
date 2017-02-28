import unittest
import unittest.mock as mock
#import redis_map
import webapi_server
from collections import defaultdict

class TestRequestId(unittest.TestCase):
    def test_reqid_spaces(self):
        url = "http://ya.ru"
        data = {"test": True}
        headers = {"User-Agent": "PINGAS"}
        method = "POST"
        self.assertEqual([i for i in webapi_server.Server._request_to_id(url, data, headers, method)].count(' '), 3)
    def test_reqid_different(self):
        url = "http://ya.ru"
        data = {"test": True}
        headers = {"User-Agent": "PONGAS"}
        method = "POST"
        id1 = webapi_server.Server._request_to_id(url, data, headers, method)
        url = "http://ya.ru"
        data = {"test": True}
        headers = {"User-Agent": "PINGAS"}
        method = "POST"
        id2 = webapi_server.Server._request_to_id(url, data, headers, method)
        self.assertNotEqual(id1, id2)

class TestRetrieveLocal(unittest.TestCase):
    @mock.patch('redis_map.RedisMap', 'dict')
    def setUp(self):
        self.server = webapi_server.Server()
        self.mock_id = mock.MagicMock(return_value="id")
        self.mock_upd = mock.MagicMock(name='update_cache')
        self.server._request_to_id = self.mock_id
        self.server._update_cache = self.mock_upd
        self.open_mock = mock.MagicMock(return_value="file")
        self.url = "http://ya.ru"
        self.headers = {"UA": "test-ua"}
        self.data = {"type": "test"}
        self.method = "GET"

    @mock.patch('os.chdir', mock.MagicMock)
    def test_local_noupdate(self):
        self.server.cache = {"id": "file.ext"}
        with mock.patch('builtins.open', self.open_mock, create=True):
            self.assertEqual(self.server._retrieve_local(self.url, self.data, self.headers, self.method), "file")
        assert not self.mock_upd.called

    @mock.patch('os.chdir', mock.MagicMock)
    def test_local_doupdate(self):
        self.server.cache = defaultdict(lambda: "file.ext")
        with mock.patch('builtins.open', self.open_mock, create=True):
            self.assertEqual(self.server._retrieve_local(self.url, self.data, self.headers, self.method), "file")
        assert self.mock_upd.called

class TestRetrieveRemote(unittest.TestCase):
    @mock.patch('redis_map.RedisMap', 'dict')
    def setUp(self):
        self.server = webapi_server.Server()

    @mock.patch('urllib.request.urlopen', lambda x: x)
    def test_post(self):
        url = "http://ya.ru"
        data = {"test": "case"}
        headers = {"ua": "mytest"}
        method = "POST"
        req = self.server._retrieve_remote(url, data, headers, method)
        self.assertEqual(url, req.full_url)

class TestProcessRequest(unittest.TestCase):
    @mock.patch('redis_map.RedisMap', 'dict')
    def setUp(self):
        self.server = webapi_server.Server()
        self.mock_local = mock.MagicMock(name='retrieve_local')
        self.mock_remote = mock.MagicMock(name='retrieve_remote')
        self.mock_update = mock.MagicMock(name='update_cache')
        self.server._retrieve_local = self.mock_local
        self.server._retrieve_remote = self.mock_remote
        self.server._update_cache = self.mock_update

    def test_remote_pure(self):
        request = {
            "name": "test_method"
        }
        schema = {
            "test_method": {
                "url": "http://ya.ru",
                "headers": {
                    "header": "header_value"
                },
                "data": {
                    "data": "data_value"
                },
                "method": "GET"
            }
        }
        self.server._init_schema(schema)
        self.server._process_request(request)
        #self.mock_local.assert_any_call()
        assert self.mock_remote.called

    def test_local_pure(self):
        request = {
            "name": "test_cached"
        }
        schema = {
            "test_cached": {
                "url": "http://ya.ru",
                "headers": {
                    "header": "header_value"
                },
                "data": {
                    "data": "data_value"
                },
                "method": "GET",
                "cache": True
            }
        }
        self.server._init_schema(schema)
        self.server._process_request(request)
        assert self.mock_local.called

    def test_refresh_pure(self):
        request = {
            "name": "test_cached",
            "update": True
        }
        schema = {
            "test_cached": {
                "url": "http://ya.ru",
                "headers": {
                    "header": "header_value"
                },
                "data": {
                    "data": "data_value"
                },
                "method": "GET",
                "cache": True
            }
        }
        self.server._init_schema(schema)
        self.server._process_request(request)
        assert self.mock_update.called