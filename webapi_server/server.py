import io
import json
import logging
import os
import random
import socket
import string
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from functools import partial
import yaml
from redis_map import RedisMap

def random_string(alphabet, length):
    result = ""
    for i in range(length):
        result += random.choice(alphabet)
    return result

class Server:
    _default_config = {
        "cache_dir": "cache",
        "redis_host": "localhost",
        "redis_port": 6379,
        "redis_name": "cache",
        "schema": "schema.yaml",
        "port": 8080,
        "host": '',
        "chunk_len": 1024 * 256,
        "random_name_len": 32,
        "max_request_len": 1024,
        "backlog": 1,
        "log_file": "server.log",
        "verbose": 0
    }

    def __init__(self, config=None):
        self.threads = []
        self.config = None
        self.root_dir = None
        self.redis_host = None
        self.redis_port = None
        self.redis_name = None
        self.cache_dir = None
        self.port = None
        self.host = None
        self.chunk_length = None
        self.name_length = None
        self.backlog = None
        self.max_length = None
        self.log_file = None
        self.verbose = None
        self.schema_file = None
        self.logger = __name__
        self.cache = {}
        self.methods = {}
        if isinstance(config, str):
            self._load_config_from_file(config)
        elif isinstance(config, dict):
            self._load_config_from_dict(config)
        else:
            self._load_config_from_dict({})

    def _init_logger(self):
        logger = logging.getLogger(self.logger)
        logger.handlers = []
        if self.log_file:
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)
        if self.verbose:
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(logging.DEBUG)
            logger.addHandler(stdout_handler)

    def _init_schema(self, schema=None):
        if not schema:
            schema = {}

        self.methods = schema
        self.cache = RedisMap(host=self.redis_host, port=self.redis_port, name=self.redis_name)

        for method_name, method in self.methods.items():
            method["args"] = {}
            method["args"]["data"] = defaultdict(list)
            method["args"]["headers"] = defaultdict(list)

            for key, value in method["data"].items():
                if not isinstance(value, str):
                    method["data"][key] = value = str(value)
                if value.startswith("$"):
                    method["args"]["data"][value[1:]].append(key)

            for key, value in method["headers"].items():
                if not isinstance(value, str):
                    method["headers"][key] = value = str(value)
                if value.startswith("$"):
                    method["args"]["headers"][value[1:]].append(key)

    def _save(self):
        pass

    def schema_from_file(self, filename):
        if not os.path.exists(filename):
            logging.warning("No schema file found, using empty schema")
            self._init_schema()
        else:
            with open(filename, 'r') as f:
                self._init_schema(yaml.load(f))

    def _load_config_from_file(self, config_fn):
        with open(config_fn, 'r') as f:
            config = yaml.load(f)
        self._load_config_from_dict(config)

    def _load_config_from_dict(self, config):
        self.config = dict(self._default_config, **config)
        self.root_dir = os.getcwd()
        self.redis_host = self.config["redis_host"]
        self.redis_port = self.config["redis_port"]
        self.redis_name = self.config["redis_name"]
        self.cache_dir = os.path.abspath(self.config["cache_dir"])
        self.port = self.config["port"]
        self.host = self.config["host"]
        self.chunk_length = self.config["chunk_len"]
        self.name_length = self.config["random_name_len"]
        self.backlog = self.config["backlog"]
        self.max_length = self.config["max_request_len"]
        self.log_file = self.config["log_file"]
        self.verbose = self.config["verbose"]

    def _apply_config(self):
        self._init_logger()
        self.schema_from_file(self.config["schema"])

    def reload_config(self, config):
        if isinstance(config, str):
            self._load_config_from_file(config)
        elif isinstance(config, dict):
            self._load_config_from_dict(config)
        else:
            raise TypeError("Unknown config type")
        self._apply_config()

    def _process_request(self, request):
        method = self.methods[request["name"]]
        url = method["url"]
        headers = method["headers"].copy()
        data = method["data"].copy()
        if "headers" in method["args"] and method["args"]["headers"]:
            headers = self._insert_arguments(args=request["args"], data=headers, map=method["args"]["headers"])
        if "data" in method["args"] and method["args"]["headers"]:
            data = self._insert_arguments(args=request["args"], data=data, map=method["args"]["data"])
        if "cache" in method and method["cache"]:
            if "update" in request and request["update"]:
                self._update_cache(url, data, headers, method["method"])
            return self._retrieve_local(url, data, headers, method["method"])
        else:
            return self._retrieve_remote(url, data, headers, method["method"])

    def _insert_arguments(self, args, data, map):
        data = data.copy()
        for arg in map:
            for loc in map[arg]:
                data[loc] = args[arg]
        return data

    def _random_name(self, directory, length=None):
        if not length:
            length = self.name_length
        os.chdir(directory)
        name = ""
        while (not name) or os.path.exists(name):
            name = random_string(string.ascii_lowercase, length)
        with open(name, 'a'):
            pass
        os.chdir(self.root_dir)
        return name

    @staticmethod
    def _request_to_id(url, data, headers, method):
        if data:
            data_arr = sorted(list(data.items()))
            data_str = urllib.parse.urlencode(data_arr)
        else:
            data_str = "None"

        if headers:
            header_arr = sorted(list(headers.items()))
            header_str = urllib.parse.urlencode(header_arr)
        else:
            header_str = "None"

        return "{0} {1} {2} {3}".format(str(url), str(data_str), str(header_str), str(method))

    def _update_cache(self, url, data, headers, method):
        logging.info("Fetching new local version...")
        with self._retrieve_remote(url, data, headers, method) as result:
            name = self._random_name(self.cache_dir)
            os.chdir(self.cache_dir)
            with open(name, 'wb') as f:
                for chunk in iter(partial(result.read, self.chunk_length), b''):
                    f.write(chunk)
            cache_id = self._request_to_id(url, data, headers, method)
            self.cache[cache_id] = name
        os.chdir(self.root_dir)

    def _retrieve_local(self, url, data, headers, method):
        cache_id = self._request_to_id(url, data, headers, method)
        if cache_id not in self.cache:
            self._update_cache(url, data, headers, method)
        else:
            logging.info("Sending local version...")
        os.chdir(self.cache_dir)
        return open(self.cache[cache_id], 'rb')

    @staticmethod
    def _retrieve_remote(url, data, headers, method):
        if method == "GET":
            url += '?' + urllib.parse.urlencode(data)
            data = None
        else:
            data = urllib.parse.urlencode(data)
        req = urllib.request.Request(url=url, data=data, headers=headers)
        return urllib.request.urlopen(req)

    def _process_connect(self, conn, addr):
        with conn:
            data = conn.recv(self.max_length)
            req = json.loads(data.decode("UTF-8"), "UTF-8")
            # noinspection PyBroadException
            try:
                file = self._process_request(req)
                status = 0
            except:
                file = io.BytesIO()
                status = 127
                logging.exception("Exception processing the request")

            logging.debug("Started sending reply...")
            now = time.time()
            conn.sendall(chr(status).encode('ascii'))
            for chunk in iter(partial(file.read, self.chunk_length), b''):
                conn.sendall(chunk)
            logging.info("Sent everything in {0} seconds".format(str(time.time() - now)))
            conn.close()
            file.close()
            return

    def run(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.host, self.port))
                s.listen(self.backlog)
                logging.info("Listening on the port {0}".format(int(self.port)))
                while True:
                    conn, addr = s.accept()
                    logging.info("New connection...")
                    thread = threading.Thread(target=self._process_connect, args=(conn, addr))
                    self.threads.append(thread)
                    thread.start()

        except KeyboardInterrupt:
            logging.info("Shutting down...")
            self._save()
