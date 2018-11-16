import hashlib
import json
import os
import threading
import time
from datetime import datetime, timedelta

from grapheneapi.websocket import Websocket
from pymongo import MongoClient

MONGO_HOST = os.environ.get('MONGO_HOST', '127.0.0.1')
MONGO_PORT = os.environ.get('MONGO_PORT', 27017)
MONGO_URL = "mongodb://{}".format(MONGO_HOST)

# block info

class WebsocketClient():
    def __init__(self):
        self.msg_id = 1

        self.api_ids = {
            # 'login': 1
        }

        self.__block_handle = ''
        self.__blockchain_init()

        self.__init_flag = True
        self.__get_api_id()
        self.__init_flag = False

    def __blockchain_init(self):
        file_name = 'host_info.json'
        path = os.path.abspath(os.path.dirname(__file__))
        file_name = os.path.join(path, file_name)
        f = open(file_name, 'r')
        str_key = f.read()
        f.close()
        key = json.loads(str_key)
        url = key['url']

        self.__block_handle = Websocket(url)
        self.__block_handle.connect()

    def __get_api_id(self):
        api = 1
        method_fun = 'login'
        params = ["", ""]
        ret = self.request(api, method_fun, params)
        # id = json.loads(ret)['result']
        # self.api_ids[method_fun] = id

        method_fun = 'database'
        params=[]
        ret = self.request(api, method_fun, params)
        id = ret[1]['result']
        self.api_ids[method_fun] = id

        method_fun = 'asset'
        ret = self.request(api, method_fun, params)
        id = ret[1]['result']
        self.api_ids[method_fun] = id

        method_fun = 'history'
        ret = self.request(api, method_fun, params)
        id = ret[1]['result']
        self.api_ids[method_fun] = id

    def __get_msg_id(self):
        self.msg_id += 1
        return self.msg_id

    def __set_msg_id(self):
        self.msg_id = 0

    def request(self, api, method_name, params):
        if not self.__init_flag:
            api_id = self.load_api_id(api)
        else:
            api_id = api
        payload = {
            'id': self.__get_msg_id(),
            'method': 'call',
            'params': [
                api_id,
                method_name,
                params
            ]
        }
        try:
            reply = self.__block_handle.rpcexec(payload)
        except Exception:
            print('WebSocketException, method:{}, params:{}'.format(method_name, str(params)))
            return False, []

        try:
            ret = json.loads(reply)
        except ValueError:
            raise ValueError("Client returned invalid format. Expected JSON!")

        if 'error' in ret:
            print('error,method:{}, params:{}, msg"{}'.format(method_name, str(params), ret))
            return False, []
        else:
            return True, ret

    def load_api_id(self, api):
        if api not in self.api_ids:
            api_id = self.request('1', api, [])
            self.api_ids[api] = api_id
        return self.api_ids[api]


client = WebsocketClient()


