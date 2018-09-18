from pymongo import MongoClient
import os
import json
import time
from grapheneapi.websocket import Websocket
from log import LogClass
import threading
from datetime import datetime, date, timedelta
import hashlib

MONGO_HOST = os.environ.get('MONGO_HOST', '127.0.0.1')
MONGO_PORT = os.environ.get('MONGO_PORT', 27017)
MONGO_URL = "mongodb://{}".format(MONGO_HOST)

# block info
Database_api_id = ''
History_api_id = ''
# Network_node_api_id = ''
# Network_broadcast_api_id = ''
# Crypto_api_id = ''
# Block_api_id = ''
Asset_api_id = ''
Orders_api_id = ''

import random


class api():
    def __init__(self):
        self.__TESTNET = 0  # todo:放入配置文件
        log_handle = LogClass('log', "explorer.log", 'INFO')
        self.run_log = log_handle.getLogging()

        self.__id_index = 0  # 索引
        self.__first_syn = True

        self.msg_id = 0
        self.lock = threading.Lock()
        self.__block_handle = ''
        self.__blockchain_init()

        dbClient = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
        table_base = 'explorer'
        table = 'asset'
        self.__db_asset = dbClient[table_base][table]

        table = 'market'
        self.__db_market = dbClient[table_base][table]

        table = 'holders'
        self.__db_holders = dbClient[table_base][table]

        table = 'referrers'
        self.__db_referrers = dbClient[table_base][table]

        table = 'operation'
        self.__db_ops = dbClient[table_base][table]

        table = 'syndb'
        self.__db_syn = dbClient[table_base][table]

    def __blockchain_init(self):
        fileName = 'host_info.json'
        path = os.path.abspath(os.path.dirname(__file__))
        fileName = os.path.join(path, fileName)
        f = open(fileName, 'r')
        strKey = f.read()
        f.close()
        key = json.loads(strKey)
        URL = key['url']

        self.__block_handle = Websocket(URL)
        self.__block_handle.connect()
        if not self.__get_api_id():
            raise "get_api_id err"

    def __get_msg_id(self):
        self.lock.acquire()
        self.msg_id += 1
        self.lock.release()
        return self.msg_id

    def __set_msg_id(self):
        self.lock.acquire()
        self.msg_id = 0
        self.lock.release()

    def __get_api_id(self):
        msg_id = self.__get_msg_id()
        try:
            payLoad = {"method": "call", "params": [1, "login", ["", ""]], "id": msg_id}
            ret = self.__block_handle.rpcexec(payLoad)
        except:
            self.run_log.info("login err")
            return False
        if ret:
            try:
                msg_id = self.__get_msg_id()
                payLoad = {"method": "call", "params": [1, "database", []], "id": msg_id}
                ret = self.__block_handle.rpcexec(payLoad)
                # print("database_api_id:%s" % ret)
                global Database_api_id
                Database_api_id = json.loads(ret)['result']
            except:
                self.run_log.info("database_api err")
                return False
            try:
                msg_id = self.__get_msg_id()
                payLoad = {"method": "call", "params": [1, "history", []], "id": msg_id}
                ret = self.__block_handle.rpcexec(payLoad)
                # print("history_api_id:%s" % ret)
                global History_api_id
                History_api_id = json.loads(ret)['result']
            except:
                self.run_log.info("history_api err")
                return False
            # try:
            #     msg_id = self.__get_msg_id()
            #     payLoad = {"method": "call", "params": [1, "network_node", []], "id": msg_id}
            #     ret = self.__block_handle.rpcexec(payLoad)
            #     print("network_node_api_id:%s" % ret)
            #     global Network_node_api_id
            #     Network_node_api_id = json.loads(ret)['result']
            # except:
            #     self.run_log.info("network_node_api err")
            #     return False
            # try:
            #     msg_id = self.__get_msg_id()
            #     payLoad = {"method": "call", "params": [1, "network_broadcast", []], "id": msg_id}
            #     ret = self.__block_handle.rpcexec(payLoad)
            #     print("network_broadcast_api_id:%s" % ret)
            #     global Network_broadcast_api_id
            #     Network_broadcast_api_id = json.loads(ret)['result']
            # except:
            #     self.run_log.info("network_broadcast_api err")
            #     return False
            # try:
            #     msg_id = self.__get_msg_id()
            #     payLoad = {"method": "call", "params": [1, "crypto", []], "id": msg_id}
            #     ret = self.__block_handle.rpcexec(payLoad)
            #     print("crypto_api_id:%s" % ret)
            #     global Crypto_api_id
            #     Crypto_api_id = json.loads(ret)['result']
            # except:
            #     self.run_log.info("crypto_api err")
            #     return False
            # try:
            #     msg_id = self.__get_msg_id()
            #     payLoad = {"method": "call", "params": [1, "block", []], "id": msg_id}
            #     ret = self.__block_handle.rpcexec(payLoad)
            #     print("block_api_id:%s" % ret)
            #     global Block_api_id
            #     Block_api_id = json.loads(ret)['result']
            # except:
            #     self.run_log.info("block_api err")
            #     return False
            try:
                msg_id = self.__get_msg_id()
                payLoad = {"method": "call", "params": [1, "asset", []], "id": msg_id}
                ret = self.__block_handle.rpcexec(payLoad)
                # print("asset_api_id:%s" % ret)
                global Asset_api_id
                Asset_api_id = json.loads(ret)['result']
            except:
                self.run_log.info("asset_api err")
                return False
            try:
                msg_id = self.__get_msg_id()
                payLoad = {"method": "call", "params": [1, "orders", []], "id": msg_id}
                ret = self.__block_handle.rpcexec(payLoad)
                # print("orders_api_id:%s" % ret)
                global Orders_api_id
                Orders_api_id = json.loads(ret)['result']
            except:
                self.run_log.info("orders_api err")
                return False
        else:
            self.run_log.info("login failure")
            return False
        return True

    def __is_object(self, str):
        return len(str.split(".")) == 3

    def __get_request_temple(self, api_id, method_fun, params):
        msg_id = self.__get_msg_id()
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [api_id, method_fun, params]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun %s err" % method_fun)
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("{} err, msg:{}".format(method_fun, ret_orgin_data))
            return False, {}
        ret_result = ret_orgin_data['result']
        return True, ret_result

    def __get_block_assert(self, asset_id):
        msg_id = self.__get_msg_id()
        method_fun = 'lookup_asset_symbols'
        try:
            if not self.__is_object(asset_id):
                pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [[asset_id], 0]]}
                ret = self.__block_handle.rpcexec(pay_load)
                # print(ret)
            else:
                method_fun = 'get_assets'
                pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [[asset_id], 0]]}
                ret = self.__block_handle.rpcexec(pay_load)
                # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun list_assets err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log("lookup_asset_symbols, err, msg:" % ret_orgin_data)
            return False, {}
        # print(ret_orgin_data)
        ret_list_account = ret_orgin_data['result'][0]

        ret_result, dynamic_asset_data = self.get_object(ret_list_account["dynamic_asset_data_id"])
        if not ret_result:
            return ret_result, dynamic_asset_data
        ret_list_account["current_supply"] = dynamic_asset_data["current_supply"]
        ret_list_account["confidential_supply"] = dynamic_asset_data["confidential_supply"]
        ret_list_account["accumulated_fees"] = dynamic_asset_data["accumulated_fees"]
        ret_list_account["fee_pool"] = dynamic_asset_data["fee_pool"]

        ret_result, issuer = self.get_object(ret_list_account["issuer"])
        if not ret_result:
            return ret_result, issuer
        ret_list_account["issuer_name"] = issuer["name"]

        return True, ret_list_account

    def __import_assert(self):
        all_asset = []
        msg_id = self.__get_msg_id()
        method_fun = 'list_assets'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, ['AAAAA', 100]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun list_assets err")
            ret = {}
            return json.dumps(ret)
        if 'error' in ret_orgin_data:
            ret_requst = {}
            return json.dumps(ret_requst)
        ret_list_account = ret_orgin_data['result']

        for i in range(len(ret_list_account)):
            all_asset.append(ret_list_account[i])

        for i in range(len(all_asset)):
            symbol = all_asset[i]['symbol']
            id_ = all_asset[i]['id']

            ret_result, ret_get_asset = self.__get_block_assert(id_)
            if not ret_result:
                return ret_result, ret_get_asset
            current_supply = ret_get_asset["current_supply"]
            precision = ret_get_asset["precision"]

            ret_result, ret_asset_holders_count = self.get_asset_holders_count(id_)
            if not ret_result:
                return ret_result, ret_asset_holders_count

            if symbol == 'BTS':
                type_ = "Core Token"
            elif all_asset[i]["issuer"] == "1.2.0":
                type_ = "SmartCoin"
            else:
                type_ = "User Issued"

            ret_result, ret_get_volume = self.get_volume('BTS', symbol)
            if not ret_result:
                return ret_result, ret_asset_holders_count

            # print symbol
            # print(ret_get_volume["quote_volume"])

            ret_result, ret_get_ticker = self.get_ticker('BTS', symbol)
            if not ret_result:
                # return ret_result, ret_get_ticker
                price = 0
                mcap = 0
            else:
                price = ret_get_ticker["latest"]
                mcap = int(current_supply) * float(price)

            if str(price) == 'inf':
                continue

            db_data = {
                'aname': symbol, 'aid': id_, 'price': price, 'volume': float(ret_get_volume['base_volume']),
                'mcap': str(mcap), 'type': type_, 'current_supply': str(current_supply),
                'holders': str(ret_asset_holders_count), 'wallettype': '', 'precision': str(precision)
            }
            if not self.__db_asset.find_one({'aid': id_}):
                self.__db_asset.insert(db_data)
            else:
                self.__db_asset.update({'aid': id_}, {'$set': db_data})

    def get_assert(self):
        try:
            count = self.__db_asset.find().count()
            assert_data = list(self.__db_asset.find().limit(count))
        except:
            count = 0
            assert_data = []
        return count, assert_data

    def __import_market(self):
        all_asset = []
        msg_id = self.__get_msg_id()
        method_fun = 'list_assets'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, ['AAAAA', 100]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun list_assets err")
            ret = {}
            return json.dumps(ret)
        if 'error' in ret_orgin_data:
            ret_requst = {}
            return json.dumps(ret_requst)
        ret_list_account = ret_orgin_data['result']

        for i in range(len(ret_list_account)):
            all_asset.append(ret_list_account[i])

        # 获取asset的symbol
        count, ret_asset_db_data = self.get_assert()
        asset_symbol = []
        for i in range(count):
            asset_symbol_tmp = []
            asset_symbol_tmp.append(ret_asset_db_data[i]['aname'])
            asset_symbol_tmp.append(ret_asset_db_data[i]['aid'])

            asset_symbol.append(asset_symbol_tmp)

        for i in range(len(all_asset)):
            symbol = all_asset[i]['symbol']
            id_ = all_asset[i]['id']

            ret_result, ret_get_volume = self.get_volume(symbol, asset_symbol[i][0])
            if not ret_result:
                return ret_result, ret_get_volume
            volume = float(ret_get_volume["base_volume"])

            ret_result, ret_get_ticker = self.get_ticker(symbol, asset_symbol[i][0])
            if not ret_result:
                price = 0
            else:
                price = ret_get_ticker['latest']

            # todo:asset_id要与asset_id对应
            db_data = {
                'asset_symbol': asset_symbol[i][0], 'symbol': symbol, 'asset_id': id_, "price": price,
                'volume': volume, 'aid': asset_symbol[i][1]
            }
            if not self.__db_market.find_one({'aid': id_}):
                self.__db_market.insert(db_data)
            else:
                self.__db_market.update({'aid': id_}, {'$set': db_data})

    def get_market(self):
        try:
            count = self.__db_market.find().count()
            market_data = list(self.__db_market.find().limit(count))
        except:
            count = 0
            market_data = {}
        return count, market_data

    def __import_holders(self):
        msg_id = self.__get_msg_id()
        method_fun = 'get_account_count'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
        except:
            self.run_log.info("fun get_account_count err")
            return False, {}
        ret_orgin_data = json.loads(ret)
        if 'error' in ret_orgin_data:
            self.run_log.info("get_object err, msg:" % ret_orgin_data)
            return False, {}
        ret_account_count = int(ret_orgin_data['result'])

        total_amount = 0
        for ac in range(0, ret_account_count):
            method_fun = 'get_objects'
            msg_id = self.__get_msg_id()
            try:
                pay_load = {"id": msg_id, "method": "call",
                            "params": [Database_api_id, method_fun, [['1.2.' + str(ac)]]]}
                ret = self.__block_handle.rpcexec(pay_load)
                ret_get_objects1 = json.loads(ret)
            except:
                self.run_log.info("fun get_objects err")
                return False, {}
            # print('get_objects', ret_get_objects1)
            accout_id = ret_get_objects1['result'][0]['id']
            account_name = ret_get_objects1['result'][0]['name']

            method_fun = 'get_account_balances'
            msg_id = self.__get_msg_id()
            try:
                pay_load = {"id": msg_id, "method": "call",
                            "params": [Database_api_id, method_fun, [accout_id, ['1.3.0']]]}
                ret = self.__block_handle.rpcexec(pay_load)
                ret_balances = json.loads(ret)
            except:
                self.run_log.info("fun get_account_balances err")
                return False, {}
            # print('balance', ret_balances)
            total_amount += float(ret_balances['result'][0]['amount'])

            method_fun = 'get_objects'
            msg_id = self.__get_msg_id()
            try:
                pay_load = {"id": msg_id, "method": "call",
                            "params": [Database_api_id, method_fun, [[ret_get_objects1['result'][0]['statistics']]]]}
                ret = self.__block_handle.rpcexec(pay_load)
                ret_get_objects2 = json.loads(ret)
            except ValueError:
                self.run_log.info("fun get_objects err")
                return False, {}
            # print('object2', ret_get_objects2)

            db_data = {
                'account_id': accout_id,
                'account_name': account_name,
                'amount': float(ret_balances['result'][0]['amount']),
                'vote_account': ret_get_objects1['result'][0]['options']['voting_account'],
                'holder_reserve': 0.0  # todo:这个字段需要后续测试
            }
            if not self.__db_holders.find_one({'account_name': account_name}):
                self.__db_holders.insert(db_data)
            else:
                self.__db_holders.update({'account_name': account_name}, {'$set': db_data})
        pass

    def get_holders(self):
        try:
            count = self.__db_holders.find().count()
            holders_data = list(self.__db_holders.find().limit(count))
        except:
            count = 0
            holders_data = []
        return count, holders_data

    def __import_referrers(self):
        msg_id = self.__get_msg_id()
        method_fun = 'get_account_count'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_account_count err")
            ret = {}
            return json.dumps(ret)
        if 'error' in ret_orgin_data:
            ret_requst = {}
            return json.dumps(ret_requst)
        ret_account_count = int(ret_orgin_data['result'])

        for ac in range(0, ret_account_count):
            ret_result, ret_get_objects1 = self.get_object('1.2.' + str(ac))
            if not ret_result:
                # print("__import_referrers, get_object err")
                continue
            account_id = ret_get_objects1['id']
            account_name = ret_get_objects1['name']

            referrer = ret_get_objects1["referrer"]
            referrer_rewards_percentage = ret_get_objects1["referrer_rewards_percentage"]
            lifetime_referrer = ret_get_objects1["lifetime_referrer"]
            lifetime_referrer_fee_percentage = ret_get_objects1["lifetime_referrer_fee_percentage"]

            db_data = {
                'account_id': account_id, 'account_name': account_name, 'referrer': referrer,
                'referrer_rewards_percentage': referrer_rewards_percentage, 'lifetime_referrer': lifetime_referrer,
                'lifetime_referrer_fee_percentage': lifetime_referrer_fee_percentage
            }

            if not self.__db_referrers.find_one({'account_id': account_id}):
                self.__db_referrers.insert(db_data)
            else:
                self.__db_referrers.update({'account_id': account_id}, {'$set': db_data})

    def get_referrers(self):
        try:
            count = self.__db_referrers.find().count()
            referres_data = list(self.__db_referrers.find().limit(count))
        except:
            count = 0
            referres_data = {}
        return count, referres_data

    def __import_operation(self, start_index):
        step = 1000
        margin = start_index + step
        for i in range(margin):
            id_ = '2.9.' + str(i + start_index)
            ret_result, ret_object = self.get_object(id_)
            if not ret_result:
                self.run_log.info("__import_operation, err")
                continue
            if ret_object == None:
                return True, []

            account_id = ret_object['account']
            op_id = ret_object['operation_id']

            ret_result, ret_account_data = self.get_account(account_id)
            if not ret_result:
                self.run_log.info("__import_operation, err")
                continue
            account_name = ret_account_data[0]['name']

            ret_result, ret_op_data = self.get_object(op_id)
            if not ret_result:
                self.run_log.info("__import_operation, err")
                continue

            block_num = ret_op_data['block_num']
            ret_result, ret_block_data = self.get_block_header(block_num)
            if not ret_result:
                self.run_log.info('__import_operation err')
                continue

            datetime = ret_block_data['timestamp']

            op_type = ret_op_data['op'][0]
            trx_in_block = ret_op_data['trx_in_block']
            op_in_trx = ret_op_data['op_in_trx']

            db_data = {
                'oh': id_, 'ath': op_id, 'block_num': block_num, 'trx_in_block': trx_in_block,
                'op_in_trx': op_in_trx, 'datetime': datetime, 'account_id': account_id,
                'op_type': op_type, 'account_name': account_name
            }

            if not self.__db_ops.find_one({'ath': op_id}):
                self.__db_ops.insert(db_data)
            else:
                self.__db_ops.update({'ath': op_id}, {'$set': db_data})

            if (i + start_index + 1) >= margin:
                margin += step

    def get_operations(self):
        try:
            count = self.__db_ops.find().count()
            referres_data = list(self.__db_ops.find().limit(count))
        except:
            count = 0
            referres_data = {}
        return count, referres_data

    def syn_last_data(self):
        count, ret_syn = self.get_syn_data()
        if count <= 0:
            index = 0
        else:
            sort_data = sorted(ret_syn, key=lambda k: k['id_index'], reverse=True)
            index = sort_data[0]['id_index'] + 1
            self.__id_index = index + 1
        if self.__first_syn:
            index = 0
            self.__first_syn = False
        if self.__syn_data_to_db(index):
            print('finish syn')

    def __syn_data_to_db(self, start_index):
        # 同步数据至数据库
        step = 1000
        margin = start_index + step
        for i in range(margin):
            id_ = '2.9.' + str(i + start_index)
            ret_result, ret_object_op = self.get_object(id_)
            if not ret_result:
                self.run_log.info("__import_operation, err")
                continue
            if ret_object_op == None:
                break

            account_id = ret_object_op['account']
            id = ret_object_op['id']
            next_id = ret_object_op['next']
            operation_id = ret_object_op['operation_id']
            sequence = ret_object_op['sequence']

            ret_result, ret_op_data = self.get_object(operation_id)
            if not ret_result:
                self.run_log.info("syn_data_to_db, err")
                continue

            block_num = ret_op_data['block_num']
            op_type = ret_op_data['op'][0]
            trx_in_block = int(ret_op_data['trx_in_block'])
            op_in_trx = ret_op_data['op_in_trx']

            ret_result, ret_block_data = self.get_block(block_num)
            if not ret_result:
                self.run_log.info('__import_operation err')
                continue
            datetime = ret_block_data['timestamp']
            transaction = ret_block_data['transactions'][trx_in_block]

            ripemd = hashlib.new("ripemd160")
            ripemd.update(json.dumps(transaction).encode('utf-8'))
            hash160 = ripemd.hexdigest()

            db_data = {
                'account_id': account_id, 'oh': id, 'next': next_id, 'op_type': op_type, 'block_num': block_num,
                'op_in_trx': op_in_trx, 'trx_in_block': trx_in_block, 'operation_id': operation_id,
                'datetime': datetime, 'sequence': sequence, 'trx_id': hash160, 'id_index': i + start_index
            }
            if not self.__db_syn.find_one({'oh': id}):
                self.__db_syn.insert(db_data)
            else:
                self.__db_syn.update({'oh': id}, {'$set': db_data})
            if (i + start_index + 1) >= margin:
                margin += step
        return True

    def get_syn_data(self):
        try:
            count = self.__db_syn.find().count()
            referres_data = list(self.__db_syn.find().limit(count))
        except:
            count = 0
            referres_data = {}
        return count, referres_data

    def run(self):
        # time.sleep(30)
        # 写入holder资产
        while True:
            self.__import_assert()
            time.sleep(3)
            self.__import_holders()
            time.sleep(3)
            self.__import_market()
            time.sleep(3)
            self.__import_referrers()
            time.sleep(3)
            self.__import_operation(self.__id_index)
            time.sleep(3)
            self.syn_last_data()
            if self.__get_msg_id() > 10000000:
                self.__set_msg_id()

    def get_header(self):
        msg_id = self.__get_msg_id()
        method_fun = 'get_dynamic_global_properties'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
        except:
            self.run_log.info("fun header err")
            return False, {}
        ret_orgin_data = json.loads(ret)
        if 'error' in ret_orgin_data:
            self.run_log.info("get_dynamic_global_properties err, msg:" % ret_orgin_data)
            return False, {}
        ret_requst = ret_orgin_data['result']

        msg_id = self.__get_msg_id()
        method_fun = 'get_witness_count'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
        except:
            self.run_log.info("fun get_witness_count err")
            return False, {}
        ret_witness_count = json.loads(ret)
        if 'error' in ret_witness_count:
            self.run_log.info("get_witness_count err, msg:" % ret_witness_count)
            return False, {}
        ret_requst['witness_count'] = ret_witness_count['result']

        msg_id = self.__get_msg_id()
        method_fun = 'get_committee_count'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
        except:
            self.run_log.info("fun get_committee_count err")
            return False, {}
        ret_committee_count = json.loads(ret)
        if 'error' in ret_committee_count:
            self.run_log.info("get_committee_count err, msg:" % ret_committee_count)
            return False, {}
        ret_requst['commitee_count'] = ret_committee_count['result']

        msg_id = self.__get_msg_id()
        method_fun = 'get_objects'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [['2.3.0']]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
        except:
            self.run_log.info("fun get_object err")
            return False, {}
        ret_get_object = json.loads(ret)
        if 'error' in ret_get_object:
            self.run_log.info("get_objects err, msg:" % ret_get_object)
            ret_requst['bts_market_cap'] = 0
        else:
            current_supply = ret_get_object['result'][0]["current_supply"]
            confidental_supply = ret_get_object['result'][0]["confidential_supply"]
            market_cap = int(current_supply) + int(confidental_supply)
            ret_requst["bts_market_cap"] = int(market_cap / 100000000)

        msg_id = self.__get_msg_id()
        method_fun = 'get_24_volume'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, ["BTS", "OPEN.BTC"]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
        except:
            self.run_log.info("fun get_24_volume err")
            return False, {}
        ret_24_volume = json.loads(ret)
        if 'error' in ret_24_volume:
            self.run_log.info("get_24_volume err, msg:" % ret_24_volume)
            ret_requst['quote_volume'] = 0
        else:
            ret_requst["quote_volume"] = ret_24_volume['result'][0]["quote_volume"]

        ret_data = sorted(ret_requst.items(), key=lambda d: d[0])
        return True, ret_data

    def get_object(self, object_id):
        msg_id = self.__get_msg_id()
        method_fun = 'get_objects'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [[object_id]]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
        except:
            self.run_log.info("fun get_objects err")
            ret = {}
            return False, json.dumps(ret)
        ret_orgin_data = json.loads(ret)
        if 'error' in ret_orgin_data:
            self.run_log.info("get_object err, msg:" % ret_orgin_data)
            return False, {}
        return True, ret_orgin_data['result'][0]

    def get_volume(self, base, quote):
        msg_id = self.__get_msg_id()
        method_fun = 'get_24_volume'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [base, quote]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_24_volume err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_24_volume err, msg:" % ret_orgin_data)
            return False, {}
        return True, ret_orgin_data['result']

    def get_ticker(self, base, quote):
        msg_id = self.__get_msg_id()
        method_fun = 'get_ticker'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [base, quote]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_ticker err")
            return False, {}

        if 'error' in ret_orgin_data:
            self.run_log.info("get_ticker err, msg:" % ret_orgin_data)
            return False, {}
        return True, ret_orgin_data['result']

    def get_asset_holders_count(self, asset_id):
        msg_id = self.__get_msg_id()
        method_fun = 'get_asset_holders_count'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Asset_api_id, method_fun, [asset_id]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun list_assets err")
            return False, json.dumps(ret)
        if 'error' in ret_orgin_data:
            self.run_log.info("get_asset_holders_count err, msg:" % ret_orgin_data)
            return False, {}
        return True, ret_orgin_data['result']

    def get_account(self, account_id):
        msg_id = self.__get_msg_id()
        method_fun = 'get_accounts'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [[account_id]]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_accounts err")
            return False, json.dumps(ret)
        if 'error' in ret_orgin_data:
            self.run_log.info("get_accounts err, msg:" % ret_orgin_data)
            return False, {}
        return True, ret_orgin_data['result']

    def get_account_name(self, account_id):
        msg_id = self.__get_msg_id()
        method_fun = 'get_accounts'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [[account_id]]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_accounts err")
            return False, json.dumps(ret)
        if 'error' in ret_orgin_data:
            self.run_log.info("get_accounts err, msg:" % ret_orgin_data)
            return False, {}
        return True, ret_orgin_data['result'][0]['name']

    def get_account_id(self, account_name):
        if not self.__is_object(account_name):
            msg_id = self.__get_msg_id()
            method_fun = 'lookup_account_names'
            try:
                pay_load = {"id": msg_id, "method": "call",
                            "params": [Database_api_id, method_fun, [[account_name], 0]]}
                ret = self.__block_handle.rpcexec(pay_load)
                # print(ret)
                ret_orgin_data = json.loads(ret)
            except:
                self.run_log.info("fun lookup_account_names err")
                return False, json.dumps(ret)
            if 'error' in ret_orgin_data:
                self.run_log.info("lookup_account_names err, msg:" % ret_orgin_data)
                return False, {}
            return True, ret_orgin_data['result'][0]['id']
        else:
            return True, account_name

    def __get_global_properties(self):
        msg_id = self.__get_msg_id()
        method_fun = 'get_global_properties'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_global_properties err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_global_properties err, msg:" % ret_orgin_data)
            return False, {}

        ret_data = ret_orgin_data['result']
        return True, ret_data

    def __add_global_informations(self, response):
        # get market cap
        ret_result, ret_object = self.get_object('2.3.0')
        if not ret_result:
            return ret_result, ret_object
        current_supply = ret_object["current_supply"]
        confidental_supply = ret_object["confidential_supply"]
        market_cap = int(current_supply) + int(confidental_supply)
        response["bts_market_cap"] = int(market_cap / 100000000)

        if self.__TESTNET != 1:  # Todo: had to do something else for the testnet
            ret_result, ret_volume = self.get_volume("BTS", "OPEN.BTC")
            if not ret_result:
                response["quote_volume"] = 0
            else:
                response["quote_volume"] = ret_volume["quote_volume"]
        else:
            response["quote_volume"] = 0

        ret_result, global_properties = self.__get_global_properties()
        if not ret_result:
            response["commitee_count"] = 0
            response["witness_count"] = 0
            return ret_result, response
        response["commitee_count"] = len(global_properties["active_committee_members"])
        response["witness_count"] = len(global_properties["active_witnesses"])
        return True, response

    def __enrich_operation(self, operation):
        msg_id = self.__get_msg_id()
        method_fun = 'get_dynamic_global_properties'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun lookup_account_names err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("lookup_account_names err, msg:" % ret_orgin_data)
            return False, {}
        ret_data = ret_orgin_data['result']
        operation["accounts_registered_this_interval"] = ret_data['accounts_registered_this_interval']

        return self.__add_global_informations(operation)

    def get_operation(self, operation_id):
        ret_result, ret_object = self.get_object(operation_id)
        if not ret_result:
            return ret_result, ret_object
        if not ret_object:
            return True, {}

        ret_result, ret_object = self.__enrich_operation(ret_object)
        if not ret_result:
            return ret_result, ret_object
        return True, ret_object

    def get_operation_full(self, operation_id):
        return self.get_operation(operation_id)

    # todo:这个函数未来可能需要，目前不做修改，修改见注释代码，根据"http://95.216.32.252:5000/get_single_operation?operation_id=1.11.0"
    # 读到的结果如下： [{'account_history': {'account': '1.2.1090', 'id': '2.9.0', 'next': '2.9.0', 'operation_id': '1.11.0', 'sequence': 1},
    # 'additional_data': {'fee_data': {'amount': 0, 'asset': '1.3.0'}, 'fill_data': {'account_id': '1.2.0', 'fill_price': '0.00000000000000000',
    # 'is_maker': True, 'order_id': '0.0.0', 'pays_amount': 0, 'pays_asset_id': '1.3.0', 'receives_amount': 0, 'receives_asset_id': '1.3.0'},
    # 'transfer_data': {'amount': 0, 'asset': '1.3.0', 'from': '1.2.0', 'to': '1.2.0'}},
    # 'block_data': {'block_num': 973, 'block_time': '2015-10-13T15:10:00', 'trx_id': '794eb2909b908c8228ad56c040329edd7a4831e8'},
    # 'operation_history': {'op': '[37,{"fee":{"amount":0,"asset_id":"1.3.0"},"deposit_to_account":"1.2.1090",
    # "balance_to_claim":"1.15.4379","balance_owner_key":"BTS7svGgH6ScN862v8SGyck7rVctfm7A2nnD8Yh7yz9ruHoW8rUU7",
    # "total_claimed":{"amount":4458,"asset_id":"1.3.0"}}]', 'op_in_trx': 0,
    # 'operation_result': '[0,{}]', 'trx_in_block': 0, 'virtual_op': 31500}, 'operation_id_num': 0, 'operation_type': 37}]
    def get_operation_full_elastic(self, operation_id):
        return self.get_operation(operation_id)
        # ret_result, ret_operation = self.get_operation(operation_id)
        # if not ret_result:
        #     return ret_result, ret_operation
        # print(ret_operation)
        #
        # operation = {
        #     "op": (ret_operation["operation_history"]["op"]),
        #     "block_num": ret_operation["block_data"]["block_num"],
        #     "op_in_trx": ret_operation["operation_history"]["op_in_trx"],
        #     "result": (ret_operation["operation_history"]["operation_result"]),
        #     "trx_in_block": ret_operation["operation_history"]["trx_in_block"],
        #     "virtual_op": ret_operation["operation_history"]["virtual_op"],
        #     "block_time": ret_operation["block_data"]["block_time"]
        # }
        # return True, operation

    def get_accounts(self):
        msg_id = self.__get_msg_id()
        method_fun = 'get_asset_holders'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Asset_api_id, method_fun, ['1.3.0', 0, 100]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_asset_holders err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_asset_holders err, msg:" % ret_orgin_data)
            return False, {}
        ret_data = ret_orgin_data['result']
        return True, ret_data

    def get_full_account(self, account_id):
        msg_id = self.__get_msg_id()
        method_fun = 'get_full_accounts'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [[account_id], 0]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_full_accounts err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_full_accounts err, msg:" % ret_orgin_data)
            return False, {}
        ret_data = ret_orgin_data['result']
        return True, ret_data

    def get_fees(self):
        return self.__get_global_properties()

    def get_block_header(self, block_num):
        msg_id = self.__get_msg_id()
        method_fun = 'get_block_header'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [block_num, 0]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_block_header err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_block_header err, msg:" % ret_orgin_data)
            return False, {}
        ret_data = ret_orgin_data['result']
        return True, ret_data

    def get_account_history(self, account_id):
        ret_result, account_id = self.get_account_id(account_id)
        if not ret_result:
            return ret_result, account_id

        msg_id = self.__get_msg_id()
        method_fun = 'get_account_history'
        try:
            pay_load = {"id": msg_id, "method": "call",
                        "params": [History_api_id, method_fun, [account_id, '1.11.1', 20, '1.11.999999999']]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_account_history err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_account_history err, msg:" % ret_orgin_data)
            return False, {}
        account_history = ret_orgin_data['result']

        if len(account_history) > 0:
            for transaction in account_history:
                ret_result, ret_block_header = self.get_block_header(transaction['block_num'])
                if not ret_result:
                    transaction["timestamp"] = 0
                    transaction["witness"] = 0
                    continue
                transaction["timestamp"] = ret_block_header["timestamp"]
                transaction["witness"] = ret_block_header["witness"]
        return True, account_history

    def get_asset(self, asset_id):
        return self.__get_block_assert(asset_id)

    def get_block(self, block_num):
        msg_id = self.__get_msg_id()
        method_fun = 'get_block'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [block_num, 0]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_block err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_block err, msg:" % ret_orgin_data)
            return False, {}
        ret_data = ret_orgin_data['result']
        return True, ret_data

    def get_last_network_ops(self):
        count, ret_ops_data = self.get_operations()
        if count <= 0:
            return True, []

        ret_data = sorted(ret_ops_data, key=lambda k: k['block_num'], reverse=True)
        ret_datas = []
        for i in range(count):
            ret_data[i].pop('_id')
            ret_datas.append('', [ret_data[i]['oh'], ret_data[i]['ath'], ret_data[i]['block_num'],
                                  ret_data[i]['trx_in_block'], ret_data[i]['op_in_trx'],
                                  ret_data[i]['datetime'], ret_data[i]['account_id'], ret_data[i]['op_type'],
                                  ret_data[i]['account_name']])
        return True, ret_datas

    def __ensure_asset_id(self, asset_id):
        msg_id = self.__get_msg_id()
        method_fun = 'lookup_asset_symbols'
        try:
            if not self.__is_object(asset_id):
                pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [[asset_id], 0]]}
                ret = self.__block_handle.rpcexec(pay_load)
                # print(ret)
            else:
                return True, asset_id
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun list_assets err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log("lookup_asset_symbols, err, msg:" % ret_orgin_data)
            return False, {}
        # print(ret_orgin_data)
        ret_list_account = ret_orgin_data['result'][0]
        return True, ret_list_account['id']

    def get_asset_holders(self, asset_id, start, limit):
        msg_id = self.__get_msg_id()
        method_fun = 'get_asset_holders'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Asset_api_id, method_fun, [asset_id, start, limit]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_asset_holders err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_asset_holders err, msg:" % ret_orgin_data)
            return False, {}
        ret_data = ret_orgin_data['result']
        return True, ret_data

    def get_workers(self):
        msg_id = self.__get_msg_id()
        method_fun = 'get_worker_count'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_worker_count err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_worker_count err, msg:" % ret_orgin_data)
            return False, {}
        workers_count = int(ret_orgin_data['result'])

        if workers_count <= 0:
            return True, []

        ret_objects = []
        for i in range(workers_count):
            result, ret_object = self.get_object('1.14.{}'.format(i))
            if not ret_object:
                continue
            ret_objects.append(ret_object)

        # get the votes of worker 1.14.0 - refund 400k
        ret_result, refund400k = self.get_object('1.14.0')
        if not ret_result:
            return ret_result, refund400k
        thereshold = int(refund400k["total_votes_for"])

        result = []
        for worker in ret_objects:
            if worker:
                ret_result, worker["worker_account_name"] = self.get_account_name(worker["worker_account"])
                if not ret_result:
                    continue
                current_votes = int(worker["total_votes_for"])
                perc = (current_votes * 100) / thereshold
                worker["perc"] = perc
                result.append([worker])

        result = result[::-1]  # Reverse list.
        return True, result

    def __ensure_safe_limit(self, limit):
        if not limit:
            limit = 10
        elif int(limit) > 50:
            limit = 50
        return limit

    def get_order_book(self, base, quote, limit=False):
        limit = self.__ensure_safe_limit(limit)
        msg_id = self.__get_msg_id()
        method_fun = 'get_order_book'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [base, quote, limit]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
        except:
            self.run_log.info("fun get_order_book err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_order_book err, msg:" % ret_orgin_data)
            return False, {}
        ret_order_book = ret_orgin_data['result']
        return True, ret_order_book

    def get_margin_positions(self, account_id):
        api_id = Database_api_id
        method_fun = 'get_margin_positions'
        params = [account_id]
        ret_result, ret_data = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info("fun get_margin_positions err")
        return True, ret_data

    def get_witnesses(self):
        api_id = Database_api_id
        method_fun = 'get_witness_count'
        params = ['']
        ret_result, witnesses_count = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info("fun get_witness_count err")

        witnesses = []
        for i in range(0, witnesses_count):
            api_id = Database_api_id
            method_fun = 'get_objects'
            params = [['1.6.{}'.format(i)]]
            ret_result, witness = self.__get_request_temple(api_id, method_fun, params)
            if not ret_result or witness == [None]:
                continue
            witnesses.append(witness)

        result = []
        for witness in witnesses:
            if witness:
                ret_result, ret_accout_name = self.get_account_name(witness[0]["witness_account"])
                if not ret_result:
                    continue
                witness[0]["witness_account_name"] = ret_accout_name
                result.append([witness[0]])

        result = sorted(result, key=lambda k: int(k[0]['total_votes']))
        result = result[::-1]  # Reverse list.
        return True, result

    def get_committee_members(self):
        api_id = Database_api_id
        method_fun = 'get_committee_count'
        params = ['']
        ret_result, committee_count = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info("fun get_committee_count err")

        committee_members = []
        for i in range(0, committee_count):
            api_id = Database_api_id
            method_fun = 'get_objects'
            params = [['1.5.{}'.format(i)]]
            ret_result, committee_member = self.__get_request_temple(api_id, method_fun, params)
            if not ret_result:
                continue
            committee_members.append(committee_member)

        result = []
        for committee_member in committee_members:
            if committee_member:
                ret_result, ret_accout_name = self.get_account_name(committee_member[0]["committee_member_account"])
                if not ret_result:
                    continue
                committee_member[0]["committee_member_account_name"] = ret_accout_name
                result.append([committee_member[0]])

        result = sorted(result, key=lambda k: int(k[0]['total_votes']))
        result = result[::-1]  # Reverse list.
        return True, result

    def get_market_chart_dates(self):
        base = datetime.date.today()
        date_list = [base - datetime.timedelta(days=x) for x in range(0, 100)]
        date_list = [d.strftime("%Y-%m-%d") for d in date_list]
        return True, list(reversed(date_list))

    def get_market_chart_data(self, base, quote):
        api_id = Database_api_id
        method_fun = 'lookup_asset_symbols'
        params = [[base], 0]
        ret_result, ret_base_data = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info("fun lookup_asset_symbols err")
            return ret_result, ret_base_data
        base_id = ret_base_data[0]["id"]
        base_precision = 10 ** float(ret_base_data[0]["precision"])

        params = [[quote], 0]
        ret_result, ret_quote_data = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result or ret_quote_data == [None]:
            self.run_log.info("fun lookup_asset_symbols err")
            return ret_result, ret_quote_data
        quote_id = ret_quote_data[0]["id"]
        quote_precision = 10 ** float(ret_quote_data[0]["precision"])

        api_id = History_api_id
        method_fun = 'get_market_history'
        now = datetime.date.today()
        ago = now - datetime.timedelta(days=100)
        params = [base_id, quote_id, 86400, ago.strftime("%Y-%m-%dT%H:%M:%S"), now.strftime("%Y-%m-%dT%H:%M:%S")]
        ret_result, market_history = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info("fun get_market_history err")
            return ret_result, ret_quote_data
        data = []
        for market_operation in market_history:
            open_quote = float(market_operation["open_quote"])
            high_quote = float(market_operation["high_quote"])
            low_quote = float(market_operation["low_quote"])
            close_quote = float(market_operation["close_quote"])

            open_base = float(market_operation["open_base"])
            high_base = float(market_operation["high_base"])
            low_base = float(market_operation["low_base"])
            close_base = float(market_operation["close_base"])

            open = 1 / (float(open_base / base_precision) / float(open_quote / quote_precision))
            high = 1 / (float(high_base / base_precision) / float(high_quote / quote_precision))
            low = 1 / (float(low_base / base_precision) / float(low_quote / quote_precision))
            close = 1 / (float(close_base / base_precision) / float(close_quote / quote_precision))

            ohlc = [open, close, low, high]

            data.append(ohlc)

        append = [0, 0, 0, 0]
        if len(data) < 99:
            complete = 99 - len(data)
            for c in range(0, complete):
                data.insert(0, append)

        return True, data

    def __get_formatted_proxy_votes(self, proxies, vote_id):
        return list(map(lambda p: '{}:{}'.format(p['id'], 'Y' if vote_id in p["options"]["votes"] else '-'), proxies))

    def get_top_proxies(self):
        count, ret_holders_data = self.get_holders()
        ret_data_tmp = []
        ret_data = []
        total_amount = 0
        ret_request = []
        if count > 0:
            for i in range(count):
                ret_data_tmp.append(ret_holders_data[i]['vote_account'])
                ret_data_tmp.append(ret_holders_data[i]['account_name'])
                ret_data_tmp.append(float(ret_holders_data[i]['amount']))
                ret_data_tmp.append(float(ret_holders_data[i]['holder_reserve']))
                ret_data_tmp.append(0.0)
                ret_data.append(ret_data_tmp)
                ret_data_tmp = []
                total_amount += float(ret_holders_data[i]['amount'])

            for i in range(len(ret_data)):
                ret_data[i][4] = float(ret_data[i][2] * 100 / total_amount)
            ret_request = sorted(ret_data, key=lambda k: -k[2])
        return True, ret_request

    def get_witnesses_votes(self):
        ret_result, top_proxies = self.get_top_proxies()
        if not ret_result:
            return ret_result, top_proxies
        top_proxies = top_proxies[:10]

        proxies = []
        for p in top_proxies:
            ret_result, proxes = self.get_object(p[0])
            if not ret_result:
                continue
            proxies.append(proxes)

        ret_result, witnesses = self.get_witnesses()
        if not ret_result:
            return ret_result, witnesses
        witnesses = witnesses[:25]  # FIXME: Witness number is variable.

        witnesses_votes = []
        for witness in witnesses:
            vote_id = witness[0]["vote_id"]
            id_witness = witness[0]["id"]
            witness_account_name = witness[0]["witness_account_name"]
            proxy_votes = self.__get_formatted_proxy_votes(proxies, vote_id)

            witnesses_votes.append([witness_account_name, id_witness] + proxy_votes)

        return True, witnesses_votes

    def get_workers_votes(self):
        ret_result, top_proxies = self.get_top_proxies()
        if not ret_result:
            return ret_result, top_proxies

        top_proxies = top_proxies[:10]
        proxies = []
        for p in top_proxies:
            ret_result, proxes = self.get_object(p[0])
            if not ret_result:
                continue
            proxies.append(proxes)

        ret_result, workers = self.get_workers()
        if not ret_result:
            return ret_result, workers
        workers = workers[:30]

        workers_votes = []
        for worker in workers:
            vote_id = worker[0]["vote_for"]
            id_worker = worker[0]["id"]
            worker_account_name = worker[0]["worker_account_name"]
            worker_name = worker[0]["name"]
            proxy_votes = self.__get_formatted_proxy_votes(proxies, vote_id)

            workers_votes.append([worker_account_name, id_worker, worker_name] + proxy_votes)

        return True, workers_votes

    def get_committee_votes(self):
        ret_result, top_proxies = self.get_top_proxies()
        if not ret_result:
            return ret_result, top_proxies

        top_proxies = top_proxies[:10]
        proxies = []
        for p in top_proxies:
            ret_result, proxes = self.get_object(p[0])
            if not ret_result:
                continue
            proxies.append(proxes)

        ret_result, committee_members = self.get_committee_members()
        if not ret_result:
            return ret_result, committee_members
        committee_members = committee_members[:11]

        committee_votes = []
        for committee_member in committee_members:
            vote_id = committee_member[0]["vote_id"]
            id_committee = committee_member[0]["id"]
            committee_account_name = committee_member[0]["committee_member_account_name"]
            proxy_votes = self.__get_formatted_proxy_votes(proxies, vote_id)

            committee_votes.append([committee_account_name, id_committee] + proxy_votes)

        return True, committee_votes

    def __static_ops(self, list_ops, op_type):
        index = -1
        for i in range(len(list_ops)):
            if op_type == list_ops[i][0]:
                list_ops[i][1] += 1
                index = i
                break
        if index == -1:
            op = [op_type, 1]
            list_ops.append(op)
        return list_ops

    def get_top_operations(self):
        count, ret_ops_data = self.get_operations()
        if count <= 0:
            return True, []

        ops = []
        for i in range(count):
            ops = self.__static_ops(ops, ret_ops_data[i]['op_type'])

        ret_op = sorted(ops, key=lambda k: k[1])
        return True, ret_op[0]

    def get_last_network_transactions(self):
        count, ret_ops_data = self.get_operations()
        if count <= 0:
            return True, []

        ret_data = sorted(ret_ops_data, key=lambda k: k['block_num'], reverse=True)
        ret_datas = []
        for i in range(min(20, count)):
            ret_data[i].pop('_id')
            ret_datas.append(['', ret_data[i]['oh'], ret_data[i]['ath'], ret_data[i]['block_num'],
                              ret_data[i]['trx_in_block'], ret_data[i]['op_in_trx'],
                              ret_data[i]['datetime'], ret_data[i]['account_id'], ret_data[i]['op_type'],
                              ret_data[i]['account_name']])
        return True, ret_datas

    def lookup_accounts(self, start):
        api_id = Database_api_id
        method_fun = 'lookup_accounts'
        params = [start, 1000]
        ret_result, accounts = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info("fun lookup_accounts err")
            return ret_result, accounts
        return True, accounts

    def lookup_assets(self, start):
        return {}

    def get_last_block_number(self):
        api_id = Database_api_id
        method_fun = 'get_dynamic_global_properties'
        params = ['']
        ret_result, dynamic_global_properties = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info("fun get_dynamic_global_properties err")
            return ret_result, dynamic_global_properties
        return True, dynamic_global_properties["head_block_number"]

    def get_account_history_pager(self, account_id, page):
        ret_result, account_id = self.get_account_id(account_id)
        if not ret_result:
            return ret_result, account_id

        # need to get total ops for account
        api_id = Database_api_id
        method_fun = 'get_accounts'
        params = [[account_id]]
        ret_result, ret_account = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result or ret_account == [None]:
            self.run_log.info('get_account_history_pager err')
            return ret_result, ret_account
        account = ret_account[0]

        ret_result, statistics = self.get_object(account["statistics"])
        if not ret_result:
            return ret_result, statistics

        total_ops = statistics["total_ops"]
        start = total_ops - (20 * int(page))
        stop = total_ops - (40 * int(page))

        if stop < 0:
            stop = 0

        if start > 0:
            api_id = History_api_id
            method_fun = 'get_relative_account_history'
            params = [account_id, stop, 20, start]
            ret_result, account_history = self.__get_request_temple(api_id, method_fun, params)
            if not ret_result:
                self.run_log.info('get_accounts err')
                return ret_result, account_history

            for transaction in account_history:
                ret_result, block_header = self.get_block_header(transaction["block_num"])
                if not ret_result:
                    continue
                transaction["timestamp"] = block_header["timestamp"]
                transaction["witness"] = block_header["witness"]
            return True, account_history
        else:
            return False, []

    def get_account_history_pager_elastic(self, account_id, page):
        ret_result, account_id = self.get_account_id(account_id)
        if not ret_result:
            return ret_result, account_id

        from_ = int(page) * 20
        ret_result, ret_his_data = self.get_account_history_pager(account_id, from_)
        if not ret_result:
            return ret_result, ret_his_data

        j = ret_his_data
        # contents = urllib2.urlopen(
        #     config.ES_WRAPPER + "/get_account_history?account_id=" + account_id + "&from_=" + str(
        #         from_) + "&size=20&sort_by=-block_data.block_time").read()


        results = [0 for x in range(len(j))]
        for n in range(0, len(j)):
            results[n] = {"op": j[n]["operation_history"]["op"],
                          "block_num": j[n]["block_data"]["block_num"],
                          "id": j[n]["account_history"]["operation_id"],
                          "op_in_trx": j[n]["operation_history"]["op_in_trx"],
                          "result": j[n]["operation_history"]["operation_result"],
                          "timestamp": j[n]["block_data"]["block_time"],
                          "trx_in_block": j[n]["operation_history"]["trx_in_block"],
                          "virtual_op": j[n]["operation_history"]["virtual_op"]
                          }

        return True, list(results)

    def get_limit_orders(self, base, quote):
        api_id = Database_api_id
        method_fun = 'get_limit_orders'
        params = [base, quote, 100]
        ret_result, limit_orders = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info('get_limit_orders err')
            return ret_result, limit_orders
        return True, limit_orders

    def get_call_orders(self, asset_id):
        api_id = Database_api_id
        method_fun = 'get_call_orders'
        params = [asset_id, 100]
        ret_result, call_orders = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info('get_limit_orders err')
            return ret_result, call_orders
        return True, call_orders

    def get_settle_orders(self, base):
        api_id = Database_api_id
        method_fun = 'get_settle_orders'
        params = [base, 100]
        ret_result, settle_orders = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info('get_limit_orders err')
            return ret_result, settle_orders
        return True, settle_orders

    def get_fill_order_history(self, base, quote):
        api_id = Database_api_id
        method_fun = 'get_fill_order_history'
        params = [base, quote, 100]
        ret_result, fill_order_history = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info('get_fill_order_history err')
            return ret_result, fill_order_history
        return True, fill_order_history

    def get_dex_total_volume(self):
        count, ret_asset_data = self.get_assert()
        if count <= 0:
            return False, {}

        usd_price = 0
        cny_price = 0
        volume = 0
        market_cap = 0
        for i in range(count):
            if ret_asset_data[i]['aname'] == 'USD':
                usd_price = float(ret_asset_data[i]['price'])
            if ret_asset_data[i]['aname'] == 'CNY':
                cny_price = float(ret_asset_data[i]['price'])
            if ret_asset_data[i]['aname'] != 'BTS':
                volume += ret_asset_data[i]['volume']
            market_cap += float(ret_asset_data[i]['mcap'])

        res = {"volume_bts": round(volume), "volume_usd": 0 if usd_price == 0 else round(volume / usd_price),
               "volume_cny": 0 if cny_price == 0 else round(volume / cny_price),
               "market_cap_bts": round(market_cap),
               "market_cap_usd": 0 if usd_price == 0 else round(market_cap / usd_price),
               "market_cap_cny": 0 if cny_price == 0 else round(market_cap / cny_price)}
        return True, res

    def get_daily_volume_dex_dates(self):
        base = datetime.date.today()
        date_list = [base - datetime.timedelta(days=x) for x in range(0, 60)]
        date_list = [d.strftime("%Y-%m-%d") for d in date_list]
        return True, list(reversed(date_list))

    def get_daily_volume_dex_data(self):
        # todo:需要按时间排序，现在没有时间这个字段
        count, ret_asset_db = self.get_assert()
        if count <= 0:
            return True, []

        dex_data = []
        for i in range(count):
            if ret_asset_db[i]['aname'] != 'BTS':
                dex_data.append(ret_asset_db[i]['volume'])

        return True, dex_data

    def get_all_asset_holders(self, asset_id):
        ret_result, asset_id = self.__ensure_asset_id(asset_id)

        all = []

        ret_result, asset_holders = self.get_asset_holders(asset_id, 0, 100)
        if not ret_result:
            return ret_result, asset_holders

        all.extend(asset_holders)

        len_result = len(asset_holders)
        start = 100
        while len_result == 100:
            start = start + 100
            ret_result, asset_holders = self.get_asset_holders(asset_id, start, 100)
            if not ret_result:
                self.run_log.info('get_all_asset_holders err:{}'.format(len_result))
                continue
            len_result = len(asset_holders)
            all.extend(asset_holders)

        return True, all

    def get_referrer_count(self, account_id):
        ret_result, account_id = self.get_account_id(account_id)
        if not ret_result:
            return ret_result, account_id

        count, ret_ref_data = self.get_referrers()
        if count <= 0:
            return True, 0

        results = 0
        for i in range(count):
            if ret_ref_data[i]['referrer'] == account_id:
                results += 1
        return True, results

    def get_all_referrers(self, account_id, page=0):
        ret_result, account_id = self.get_account_id(account_id)
        if not ret_result:
            return ret_result, account_id

        count, ret_ref_data = self.get_referrers()
        if count <= 0:
            return True, 0

        ret_ref_data.reverse()

        ref_datas = []
        for i in range(count):
            if ret_ref_data[i]['referrer'] == account_id:
                ret_ref_data[i].pop('_id')
                ref_datas.append(ret_ref_data[i])

        offset = int(page) * 20
        if offset > len(ref_datas):
            return True, ref_datas
        else:
            results = []
            if offset > len(ref_datas):
                return True, []
            else:
                for i in range(20):
                    results = ['', ref_datas[offset + i]['account_id'], ref_datas[offset + i]['account_name'],
                               ref_datas[offset + i]['lifetime_referrer'],
                               ref_datas[offset + i]['lifetime_referrer_fee_percentage'],
                               ref_datas[offset + i]['referrer'], ref_datas[offset + i]['referrer_rewards_percentage']]
                    if (offset + i + 1) >= len(ref_datas):
                        break
                return True, results

    def get_grouped_limit_orders(self, base, quote, group=10, limit=False):
        limit = self.__ensure_safe_limit(limit)

        ret_result, base = self.__ensure_asset_id(base)
        if not ret_result:
            self.run_log.info('get_grouped_limit_orders base_id asset err')
            return ret_result, base
        ret_result, quote = self.__ensure_asset_id(quote)
        if not ret_result:
            self.run_log.info('get_grouped_limit_orders quote_id asset err')
            return ret_result, quote

        api_id = Orders_api_id
        method_fun = 'get_grouped_limit_orders'
        params = [base, quote, group, None, limit]
        ret_result, grouped_limit_orders = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info('get_grouped_limit_orders err')
            return ret_result, grouped_limit_orders
        return True, grouped_limit_orders

    def get_account_history_by_operations(self, account_id):
        api_id = History_api_id
        method_fun = 'get_account_history_by_operations'
        params = [account_id, [0, 100], 0, 20]
        ret_result, grouped_limit_orders = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info('get_account_history_by_operations err')
            return ret_result, grouped_limit_orders
        return True, grouped_limit_orders

    def __get_history_info(self, op_id):
        ret_result, ret_account_his = self.get_object(op_id)
        if not ret_result:
            return ret_result, ret_account_his

        id_ = ret_account_his['id']

        account_his_info = {'account': ret_account_his['account'], 'id': op_id,
                            'next': ret_account_his['next'], 'operation_id': ret_account_his['operation_id'],
                            'sequence': ret_account_his['sequence']}

        return True, account_his_info

    def __get_additional_data(self, account_id, account_info):
        op_info = account_info[1]
        fee_data = dict(amount=op_info['fee']['amount'], asset=op_info['fee']['asset_id'])

        # todo :hardcode 需要改进
        account_id = account_id
        if 'order_id' in op_info:
            order_id = op_info['order_id']
        else:
            order_id = '0.0.0'
        if 'is_make' in op_info:
            is_maker = op_info['is_maker']
        else:
            is_maker = 'true'
        if 'pays' in op_info:
            pays_amount = op_info['pays']['amount']
            pays_asset_id = op_info['pays']['asset_id']
            receives_amount = op_info['receives']['amount']
            receives_asset_id = op_info['receives']['receives_asset_id']
        else:
            pays_amount = 0
            pays_asset_id = '1.3.0'
            receives_amount = 0
            receives_asset_id = '1.3.0'

        if 'fill_price' in op_info:
            fill_price = float(op_info['fill_price']['base']['amount']) / float(
                op_info['fill_price']['quote']['amount'])
        else:
            fill_price = 0

        fill_data = dict(account_id=account_id, fill_price=fill_price, is_maker=is_maker,
                         pays_amount=pays_amount, pays_asset_id=pays_asset_id,
                         receives_amount=receives_amount, receives_asset_id=receives_asset_id)
        # transfer_data = {'amount': op_info['amount']['amount'], 'asset': op_info['amount']['asset_id'],
        #                  'from': op_info['from'], 'to': op_info['to']}
        transfer_data = {'amount': 0, 'asset': '1.3.0', 'from': '1.2.0', 'to': '1.2.0'}  # todo:从explorer返回的数据像硬编码，后面再改

        addtional_data = {"fee_data": fee_data, "fill_data": fill_data, "transfer_data": transfer_data}
        return True, addtional_data

    def __get_operation_history(self, his_info):
        op_info = his_info['op']
        operation_history = {'op': op_info, 'op_in_trx': his_info['op_in_trx'], 'operation_result': his_info['result'],
                             'trx_in_block': his_info['trx_in_block'], 'virtual_op': his_info['virtual_op']}
        return True, operation_history

    def __get_trx_id(self, trx):
        api_id = Database_api_id
        method_fun = 'get_transaction_hex'
        params = trx
        ret_result, ret_trx_data = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            return ret_result, ret_trx_data
        return True, ret_trx_data

    def __get_block_info(self, block_num, trx_index):
        ret_result, ret_block = self.get_block(block_num)
        if not ret_result:
            return ret_result, ret_block

        ret_result, ret_trx_id = self.__get_trx_id(ret_block[trx_index])

        block_info = {
            'block_num': block_num, 'block_time': ret_block['timestamp'],
            'trx_id': ret_trx_id  # todo: trx_id暂时用ripemd160计算json得到
        }
        return True, block_info

    def get_account_history_elastic(self, size, start):
        count, ret_ops_data = self.get_syn_data()
        if count <= 0:
            return count, []

        ret_opts_sort = sorted(ret_ops_data, key=lambda k: k['id_index'], reverse=True)
        ret_opts_sort = ret_opts_sort[start:min(size, count)]
        history_elastic = []
        for i in range(len(ret_opts_sort)):
            op_id = ret_opts_sort[i]['oh']
            ath = ret_opts_sort[i]['operation_id']
            block_num = ret_opts_sort[i]['block_num']
            account_id = ret_opts_sort[i]['account_id']
            ret_result, ret_objects_op = self.get_object(ath)
            if not ret_result:
                self.run_log.info('get_account_history_elastic err')
                continue

            ret_his_op = ret_objects_op
            block_info = {
                'block_num': block_num, 'block_time': ret_opts_sort[i]['datetime'],
                'trx_id': ret_opts_sort[i]['trx_id']
            }

            account_history_info = {
                'account': account_id, 'id': op_id, 'next': ret_opts_sort[i]['next'],
                'operation_id': ath, 'sequence': ret_opts_sort[i]['sequence']}

            ret_result, additional_data = self.__get_additional_data(account_id, ret_his_op['op'])
            if not ret_result:
                continue

            ret_result, operation_history = self.__get_operation_history(ret_his_op)
            if not ret_result:
                continue

            operation_id_num = int(op_id.split('.')[2])
            operation_type = operation_history['op'][0]

            dict_tmp = {'account_history': account_history_info, 'additional_data': additional_data,
                        'block_data': block_info, 'operation_history': operation_history,
                        'operation_id_num': operation_id_num, 'operation_type': operation_type}

            history_elastic.append(dict_tmp)
        return True, history_elastic

    def compare_time(self, time1, time2):
        s_time = time.mktime(time.strptime(time1, '%Y-%m-%d'))
        e_time = time.mktime(time.strptime(time2, '%Y-%m-%d'))
        return int(s_time) - int(e_time)

    def get_account_history_elastic2(self, from_date, to_date, type, agg_field, size):

        # 取size 个
        count, ret_syn_db = self.get_syn_data()
        if count <= 0:
            return True, []
        sort_data = sorted(ret_syn_db, key=lambda k: k['id_index'], reverse=True)
        days = from_date.split('-')[1]
        days = days[0:(len(days) - 1)]
        start_data = date.today() + timedelta(days=-int(days))

        end_index = 0
        for i in range(count):
            time_date = sort_data[i]['datetime'].split('T')[0]
            if self.compare_time(time_date, str(start_data)) < 0:
                end_index = i
                break
            else:
                end_index += 1

        ret_sort_data = sort_data[:min(size, end_index)]
        list_ops = []
        if agg_field == 'operation_type':
            for i in range(len(ret_sort_data)):
                list_ops = self.__static_ops(list_ops, ret_sort_data[i]['op_type'])
        else:
            for i in range(len(ret_sort_data)):
                list_ops = self.__static_ops(list_ops, ret_sort_data[i]['trx_id'])
        ret_data = []
        for i in range(len(list_ops)):
            ret_data.append({'doc_count': list_ops[i][1], 'key':list_ops[i][0]})
        return True, ret_data

    def get_trx(self, trx_id, size):
        count, ret_ops_data = self.get_syn_data()
        if count <= 0:
            return count, []

        ret_opts_sort = sorted(ret_ops_data, key=lambda k: k['block_num'], reverse=True)
        history_elastic = []
        for i in range(len(ret_opts_sort)):
            if ret_opts_sort[i]['trx_id'] == trx_id:
                op_id = ret_opts_sort[i]['oh']
                ath = ret_opts_sort[i]['operation_id']
                block_num = ret_opts_sort[i]['block_num']
                account_id = ret_opts_sort[i]['account_id']
                ret_result, ret_objects_op = self.get_object(ath)
                if not ret_result:
                    self.run_log.info('get_account_history_elastic err')
                    continue

                ret_his_op = ret_objects_op
                block_info = {
                    'block_num': block_num, 'block_time': ret_opts_sort[i]['datetime'],
                    'trx_id': ret_opts_sort[i]['trx_id']
                }

                account_history_info = {'account': account_id, 'id': op_id, 'next': ret_opts_sort[i]['next'],
                                        'operation_id': ath, 'sequence': ret_opts_sort[i]['sequence']}

                ret_result, additional_data = self.__get_additional_data(account_id, ret_his_op['op'])
                if not ret_result:
                    continue

                ret_result, operation_history = self.__get_operation_history(ret_his_op)
                if not ret_result:
                    continue

                operation_id_num = int(op_id.split('.')[2])
                operation_type = operation_history['op'][0]

                dict_tmp = {'account_history': account_history_info, 'additional_data': additional_data,
                            'block_data': block_info, 'operation_history': operation_history,
                            'operation_id_num': operation_id_num, 'operation_type': operation_type}

                history_elastic.append(dict_tmp)
        ret_trx_sorted = history_elastic[:min(size, len(history_elastic))]
        return True, ret_trx_sorted
