# from bitshares.block import Block
# print(Block(1))
#
# from bitshares.account import Account
# account = Account("init0")
# print(account.balances)
# print(account.openorders)
# for h in account.history():
#     print(h)


from grapheneapi import graphenewsprotocol
from grapheneapi import graphenews
from grapheneapi import grapheneapi
from grapheneapi import grapheneclient
from grapheneapi import graphenewsrpc
from grapheneapi import graphenehttprpc



import json


def login():
    _ip = "localhost"
    _port = 10110
    graphene = grapheneapi.GrapheneAPI(_ip, _port)

    payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": 2}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(ret)

    payLoad = {"method": "call", "params": [1, "database", []], "id": 3}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(ret)

    payLoad = {"method": "call", "params": [1, "history", []], "id": 4}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(ret)

    payLoad = {"method": "call", "params": [1, "network_broadcast", []], "id": 5}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(ret)

    payLoad = {"method": "call", "params": [1, "network_node", []], "id": 6}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(ret)


def connect():
    _ip = "localhost"
    _port = 10110
    graphene = grapheneapi.GrapheneAPI(_ip, _port)

    # payLoad = {"jsonrpc": "2.0", "method": "get_chain_properties", "params": [10000], "id": 15}
    payLoad = {"jsonrpc": "2.0", "method": "get_objects", "params": ["2.1.0"], "id": 15}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(json.dumps(ret))


import time


def get_database_api():
    _ip = "localhost"
    _port = 11011
    graphene = grapheneapi.GrapheneAPI(_ip, _port)

    payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": 1}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(ret)

    payLoad = {"method": "call", "params": [1, "history", []], "id": 2}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(ret)

    # time.sleep(2)
    ret_code = ret
    payLoad = {"id": 3, "method": "call", "params": [2, "get_objects", [['2.9.1']]]}
    ret = graphene.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(json.dumps(ret))


from grapheneapi.websocket import Websocket
from grapheneapi.rpc import Rpc
def ws_database_api():
    # url = 'ws://127.0.0.1:10110'
    url = 'ws://10.7.0.216:11011'
    # rpc = Rpc(url)
    handle = Websocket(url)
    handle.connect()

    payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": 1}
    ret = handle.rpcexec(payLoad)
    print(ret)

    payLoad = {"method": "call", "params": [1, "database", []], "id": 2}
    ret = handle.rpcexec(payLoad)
    print(ret)

    api_id = json.loads(ret)['result']
    payLoad = {"id": 3, "method": "call", "params": [api_id, 'get_objects', [['2.9.0']]]}
    ret = handle.rpcexec(payLoad)
    ret_data = json.loads(ret)
    print(ret_data)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    # ret_data = json.loads(ret)
    # payLoad = {"id": 3, "method": "call", "params": [api_id, 'get_transaction_hex', [ret_data['result']]]}
    # ret = handle.rpcexec(payLoad)
    # print(ret)
    # print(len(ret_data['result']))
    # print(ret_data)


def ws_blocks_api():
    url = 'ws://127.0.0.1:10110'
    # rpc = Rpc(url)
    handle = Websocket(url)
    handle.connect()

    payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": 1}
    ret = handle.rpcexec(payLoad)
    print(ret)

    payLoad = {"method": "call", "params": [1, "blocks", []], "id": 2}
    ret = handle.rpcexec(payLoad)
    print(ret)

    api_id = json.loads(ret)['result']
    payLoad = {"id": 3, "method": "call", "params": [api_id, "get_tracked_groups", []]}
    ret = handle.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(json.dumps(ret))


def ws_asset_api():
    url = 'ws://127.0.0.1:10110'
    # rpc = Rpc(url)
    handle = Websocket(url)
    handle.connect()

    payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": 1}
    ret = handle.rpcexec(payLoad)
    print(ret)

    payLoad = {"method": "call", "params": [1, "asset", []], "id": 2}
    ret = handle.rpcexec(payLoad)
    print(ret)

    api_id = json.loads(ret)['result']
    payLoad = {"id": 3, "method": "call", "params": [api_id, "get_asset_holders", ['1.3.0', 0, 100]]}
    ret = handle.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(json.dumps(ret))


def ws_network_node_api():
    url = 'ws://127.0.0.1:10110'
    # rpc = Rpc(url)
    handle = Websocket(url)
    handle.connect()

    payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": 1}
    ret = handle.rpcexec(payLoad)
    print(ret)

    payLoad = {"method": "call", "params": [1, "network_node", []], "id": 2}
    ret = handle.rpcexec(payLoad)
    print(ret)

    api_id = json.loads(ret)['result']
    payLoad = {"id": 3, "method": "call", "params": [api_id, "get_tracked_groups", []]}
    ret = handle.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(json.dumps(ret))


def ws_history_api():
    url = 'ws://127.0.0.1:10110'
    # rpc = Rpc(url)
    handle = Websocket(url)
    handle.connect()

    payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": 1}
    ret = handle.rpcexec(payLoad)
    print(ret)

    payLoad = {"method": "call", "params": [1, "history", []], "id": 2}
    ret = handle.rpcexec(payLoad)
    print(ret)

    api_id = json.loads(ret)['result']
    payLoad = {"id": 3, "method": "call", "params": [api_id, "get_account_history_by_operations", ['1.2.17', [0,100], 0,20]]}
    ret = handle.rpcexec(payLoad)
    # ret = sorted(ret.items(), key=lambda d: d[0])
    print(json.dumps(ret))
    ret_data = json.loads(ret)
    print(ret_data)

import requests
def ws_elastic():
    # url = "http://95.216.32.252:5000/get_single_operation?operation_id=1.11.0"
    # r = requests.get(url)
    # print(r.json())

    url = "http://95.216.32.252:5000/get_single_operation?operation_id=1.11.0"
    r = requests.get(url)
    print(r.json())


def get_bts_block():
    f = open("blocks.txt", "w")

    # url = 'ws://127.0.0.1:10110'
    url = 'ws://74.82.223.156:11011'
    # rpc = Rpc(url)
    handle = Websocket(url)
    handle.connect()

    payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": 1}
    ret = handle.rpcexec(payLoad)
    print(ret)

    payLoad = {"method": "call", "params": [1, "database", []], "id": 2}
    ret = handle.rpcexec(payLoad)
    print(ret)
    api_id = json.loads(ret)['result']

    for i in range(100000):
        payLoad = {"id": 3, "method": "call", "params": [api_id, 'get_block', [i + 1]]}
        ret = handle.rpcexec(payLoad)
        ret_data = json.loads(ret)['result']
        ret_transaction = ret_data['transactions']
        if len(ret_transaction) > 0:
            f.write(str(i+1) + ',')
            print(i + 1)
            continue
        if i % 100 == 0:
            print("searched : %s\r" % (i + 1),)
    f.close()
    # ret = sorted(ret.items(), key=lambda d: d[0])
    # ret_data = json.loads(ret)
    # payLoad = {"id": 3, "method": "call", "params": [api_id, 'get_transaction_hex', [ret_data['result']]]}
    # ret = handle.rpcexec(payLoad)
    # print(ret)
    # print(len(ret_data['result']))
    # print(ret_data)



if __name__ == '__main__':
    # login()
    # connect()
    # get_database_api()
    ws_database_api()
    # ws_asset_api()
    # ws_history_api()

    # ws_elastic()
    # get_bts_block()