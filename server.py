import yaml
import json
import socket
import os
import io
import urllib.request
import urllib.parse
import string
import random
from collections import defaultdict
from functools import partial
import time
import sys
import traceback
from redis_map import RedisMap
import threading

default_config = {
    "cache_dir": "cache",
    "redis_host": "localhost",
    "redis_port": 6379,
    "redis_name": "cache",
    "schema": "schema.yaml",
    "port": 8080,
    "host": '',
    "chunk_len": 1024*256,
    "random_name_len": 32,
    "max_request_len": 1024,
    "backlog": 1
}

def _get_or_default(fr, key, default):
    if key in fr:
        return fr[key]
    else:
        return default[key]

def _random_string(alphabet, length):
    result = ""
    for i in range(length):
        result += random.choice(alphabet)
    return result

class Server:
    def __init__(self):
        self.methods = []

    def _init_schema(self, schema=None):
        if not schema:
            self.cache = {}
            self.methods = {}
            return

        self.methods = schema
        self.cache = RedisMap(host=self.redis_host, port=self.redis_port, name=self.redis_name)

        for meth_name, method in self.methods.items():
            method["args"] = {}
            method["args"]["data"] = defaultdict(list)
            method["args"]["headers"] = defaultdict(list)

            for key, value in method["data"].items():
                if not value is str:
                    method["data"][key] = value = str(value)
                if value.startswith("$"):
                    method["args"]["data"][value[1:]].append(key)

            for key, value in method["headers"].items():
                if not value is str:
                    method["headers"][key] = value = str(value)
                if value.startswith("$"):
                    method["args"]["headers"][value[1:]].append(key)

    def _save(self):
        pass

    def config_from_file(self, config_fn):
        with open(config_fn, 'r') as f:
            config = yaml.load(f)
        self.config_from_dict(config)

    def schema_from_file(self, filename):
        if not os.path.exists(filename):
            print("No schema file found, using empty schema")
            self.init_schema()
        with open(filename, 'r') as f:
            self.init_schema(yaml.load(f))

    def config_from_dict(self, config):
        self.root_dir = os.getcwd()
        self.redis_host = get_or_default(config, "redis_host", default_config)
        self.redis_port = get_or_default(config, "redis_port", default_config)
        self.redis_name = get_or_default(config, "redis_name", default_config)
        self.cache_dir = os.path.abspath(get_or_default(config, "cache_dir", default_config))
        self.schema_from_file(get_or_default(config, "schema", default_config))
        self.port = get_or_default(config, "port", default_config)
        self.host = get_or_default(config, "host", default_config)
        self.chunk_length = get_or_default(config, "chunk_len", default_config)
        self.name_length = get_or_default(config, "random_name_len", default_config)
        self.backlog = get_or_default(config, "backlog", default_config)
        self.max_length = get_or_default(config, "max_request_len", default_config)
        self.threads = []

    def _process_request(self, request):
        method = self.methods[request["name"]]
        url = method["url"]
        headers = method["headers"].copy()
        data = method["data"].copy()
        if "headers" in method["args"]:
            for arg in method["args"]["headers"]:
                for loc in method["args"]["headers"][arg]:
                    headers[loc] = request["args"][arg]
        if "data" in method["args"]:
            for arg in method["args"]["data"]:
                for loc in method["args"]["data"][arg]:
                    data[loc] = request["args"][arg]
        if "cache" in method and method["cache"] == 1:
            if "update" in request and request["update"] == 1:
                self.update_cache(self, url, data, headers, method["method"])
            return self.retrieve_local(url, data, headers, method["method"])
        else:
            return self.retrieve_remote(url, data, headers, method["method"])

    def _random_name(self, dir, length=None):
        if not length:
            length = self.name_length
        os.chdir(dir)
        name = ""
        while (not name) or os.path.exists(name):
            name = random_string(string.ascii_lowercase, length)
        with open(name, 'a'):
            pass
        os.chdir(self.root_dir)
        return name

    def _request_to_id(self, url, data, headers, method):
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
        print("Fetching new local version...")
        with self.retrieve_remote(url, data, headers, method) as result:
            name = self.random_name(self.cache_dir)
            os.chdir(self.cache_dir)
            with open(name, 'wb') as f:
                for chunk in iter(partial(result.read, self.chunk_length), b''):
                    f.write(chunk)
            cache_id = self.request_to_id(url, data, headers, method)
            self.cache[cache_id] = name
        os.chdir(self.root_dir)

    def _retrieve_local(self, url, data, headers, method):
        cache_id = self.request_to_id(url, data, headers, method)
        print(cache_id.__hash__())
        print(self.cache)
        if cache_id not in self.cache:
            self.update_cache(url, data, headers, method)
        else:
            print("Sending local version...")
        os.chdir(self.cache_dir)
        return open(self.cache[cache_id], 'rb')

    def _retrieve_remote(self, url, data, headers, method):
        if (method == "GET"):
            url += '?' + urllib.parse.urlencode(data)
            data = None
        req = urllib.request.Request(url=url, data=data, headers=headers)
        return urllib.request.urlopen(req)

    def _process_connect(self, conn, addr):
        with conn:
            data = conn.recv(self.max_length)
            req = json.loads(data.decode("UTF-8"), "UTF-8")
            try:
                file = self.process_request(req)
                status = 0
            except BaseException as msg:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                file = io.BytesIO()
                status = 127
                print(exc_type)
                print(exc_value)
                traceback.print_tb(exc_traceback)

            print("Started sending reply...")
            now = time.time()
            conn.sendall(chr(status).encode('ascii'))
            for chunk in iter(partial(file.read, self.chunk_length), b''):
                conn.sendall(chunk)
            print("Sent everything in {0} seconds".format(str(time.time() - now)))
            conn.close()
            file.close()
            return

    def run(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.host, self.port))
                s.listen(self.backlog)
                print("Listening on the port {0}".format(int(self.port)))
                while True:
                    conn, addr = s.accept()
                    print("New connection...")
                    thread = threading.Thread(target=self.process_connect, args=(conn, addr))
                    self.threads.append(thread)
                    thread.start()

        except KeyboardInterrupt:
            s.close()
            print("Shutting down...")
            self.save()