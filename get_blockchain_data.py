import os
from pymongo import MongoClient
import json
import hashlib
import time
from datetime import datetime
from grapheneapi.websocket import Websocket
from open_explorer_api.log import LogClass

MONGO_HOST = os.environ.get('MONGO_HOST', '127.0.0.1')
MONGO_PORT = os.environ.get('MONGO_PORT', 27017)
MONGO_URL = "mongodb://{}".format(MONGO_HOST)
Database_api_id = ''
History_api_id = ''
Asset_api_id = ''
Orders_api_id = ''


class import_db():
    def __init__(self):
        self.msg_id = 0
        log_handle = LogClass('log', "explorer.log", 'INFO')
        self.run_log = log_handle.getLogging()

        self.__id_index = 0  # 索引
        self.__first_syn = True

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

        table = 'witness'
        self.__db_witness = dbClient[table_base][table]

        table = 'committee'
        self.__db_committee = dbClient[table_base][table]

        self.syn_last_data()#同步数据

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
            print("get_api_id err")

    def __get_msg_id(self):
        self.msg_id += 1
        return self.msg_id

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
        else:
            self.run_log.info("login failure")
            return False
        return True

    def __is_object(self, str):
        return len(str.split(".")) == 3

    def __get_block_asset(self, asset_id):
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
            ret_list_account = ret_orgin_data['result'][0]
        except:
            self.run_log.info("fun list_assets err")
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log("lookup_asset_symbols, err, msg:" % ret_orgin_data)
            return False, {}

        try:
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
            ret_data = ret_list_account
        except:
            return False, {}
        return True, ret_data

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

    def __import_asset(self):
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

            ret_result, ret_get_asset = self.__get_block_asset(id_)
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

            volume = 0
            price = 0
            mcap = 0

            if str(price) == 'inf':
                continue

            db_data = {
                'aname': symbol, 'aid': id_, 'price': price, 'volume': volume,
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

            volume = 0
            price = 0 #新的block数据里面改版为0

            # todo:asset_id要与asset_id对应
            db_data = {
                'asset_symbol': asset_symbol[i][0], 'symbol': symbol, 'asset_id': id_, "price": price,
                'volume': volume, 'aid': asset_symbol[i][1]
            }
            if not self.__db_market.find_one({'aid': id_}):
                self.__db_market.insert(db_data)
            else:
                self.__db_market.update({'aid': id_}, {'$set': db_data})

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
            return False, []
        if 'error' in ret_orgin_data:
            self.run_log.info("get_accounts err, msg:" % ret_orgin_data)
            return False, {}
        return True, ret_orgin_data['result']

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

    def get_syn_data(self):
        try:
            count = self.__db_syn.find().sort("block_num", -1).count()
            if count > 3000:
                count = 3000
            referres_data = list(self.__db_syn.find().sort("block_num", -1).limit(count))
        except:
            count = 0
            referres_data = {}
        for i in range(count):
            referres_data[i].pop("timestamp")
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
            self.__first_syn = False
        if self.__syn_data_to_db(index):
            print('finish syn')

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

            date_time = ret_block_data['timestamp']
            transaction = ret_block_data['transactions'][trx_in_block]

            ripemd = hashlib.new("ripemd160")
            ripemd.update(json.dumps(transaction).encode('utf-8'))
            hash160 = ripemd.hexdigest()

            timestamp = int(datetime.strptime(date_time, "%Y-%m-%dT%H:%M:%S").timestamp())

            db_data = {
                'account_id': account_id, 'oh': id, 'next': next_id, 'op_type': op_type, 'block_num': block_num,
                'op_in_trx': op_in_trx, 'trx_in_block': trx_in_block, 'operation_id': operation_id,
                'datetime': date_time, 'sequence': sequence, 'trx_id': hash160, 'id_index': i + start_index,
                'timestamp': timestamp
            }
            if not self.__db_syn.find_one({'oh': id}):
                self.__db_syn.insert(db_data)
            else:
                self.__db_syn.update({'oh': id}, {'$set': db_data})
            if (i + start_index + 1) >= margin:
                margin += step
        return True

    def __import_witnesses(self):
        msg_id = self.__get_msg_id()
        method_fun = 'get_witness_count'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, []]}
            ret = self.__block_handle.rpcexec(pay_load)
            ret_orgin_data = json.loads(ret)
            witnesses_count = ret_orgin_data['result']
        except:
            self.run_log.info("fun get_witnesses err")
            return False, []

        try:
            wit_count = int(witnesses_count)
        except:
            self.run_log.info("fun get_witnesses err")
            return False, []
        for i in range(wit_count):
            id_ = '1.5.{}'.format(i)
            ret_result, witness = self.get_object(id_)
            if not ret_result or witness == None:
                continue

            msg_id = self.__get_msg_id()
            method_fun = 'get_accounts'
            try:
                pay_load = {"id": msg_id, "method": "call",
                            "params": [Database_api_id, method_fun, [[witness["witness_account"]]]]}
                ret = self.__block_handle.rpcexec(pay_load)
                # print(ret)
                ret_orgin_data = json.loads(ret)
                witness["witness_account_name"] = ret_orgin_data['result'][0]['name']
            except Exception as e:
                self.run_log.info("fun get_witnesses err, witness:{}".format(witness))
                continue

            if not self.__db_witness.find_one({"id": id_}):
                self.__db_witness.insert(witness)
            else:
                self.__db_witness.update({'id': id_}, {'$set': witness})

    def __get_request_temple(self, api_id, method_fun, params):
        msg_id = self.__get_msg_id()
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [api_id, method_fun, params]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
            ret_result = ret_orgin_data['result']
        except:
            self.run_log.info("fun %s err" % method_fun)
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("{} err, msg:{}".format(method_fun, ret_orgin_data))
            return False, {}
        return True, ret_result

    def __import_committee(self):
        api_id = Database_api_id
        method_fun = 'get_committee_count'
        params = ['']
        ret_result, committee_count = self.__get_request_temple(api_id, method_fun, params)
        if not ret_result:
            self.run_log.info("fun get_committee_count err")
            return False, []

        com_count = int(committee_count)
        for i in range(com_count):
            id_ = "1.4.{}".format(i)
            ret_result, committee_member = self.get_object(id_)
            if not ret_result:
                continue

            msg_id = self.__get_msg_id()
            method_fun = 'get_accounts'
            try:
                pay_load = {"id": msg_id, "method": "call",
                            "params": [Database_api_id, method_fun, [[committee_member["committee_member_account"]]]]}
                ret = self.__block_handle.rpcexec(pay_load)
                # print(ret)
                ret_orgin_data = json.loads(ret)
                committee_member["committee_member_account_name"] = ret_orgin_data['result'][0]['name']
            except Exception as e:
                self.run_log.info("fun get_committee_count err, witness:{}".format(committee_member))
                continue
            if not self.__db_committee.find_one({"id": id_}):
                self.__db_committee.insert(committee_member)
            else:
                self.__db_committee.update({'id': id_}, {'$set': committee_member})

    def run(self):
        # time.sleep(30)
        # 写入holder资产
        while True:
            try:
                self.__import_asset()
                time.sleep(3)
                self.__import_holders()
                time.sleep(3)
                self.__import_market()
                time.sleep(3)
                self.__import_referrers()
                time.sleep(3)
                self.__import_operation(self.__id_index)
                time.sleep(3)
                self.__import_witnesses()
                time.sleep(3)
                self.__import_committee()
                time.sleep(3)
                self.syn_last_data()
            except:
                continue
            if self.__get_msg_id() > 10000000:
                self.msg_id = 0

    def get_object(self, object_id):
        msg_id = self.__get_msg_id()
        method_fun = 'get_objects'
        try:
            pay_load = {"id": msg_id, "method": "call", "params": [Database_api_id, method_fun, [[object_id]]]}
            ret = self.__block_handle.rpcexec(pay_load)
            # print(ret)
            ret_orgin_data = json.loads(ret)
            ret_data = ret_orgin_data['result'][0]
        except:
            self.run_log.info("fun get_objects err,id:{}".format(object_id))
            return False, {}
        if 'error' in ret_orgin_data:
            self.run_log.info("get_object err, msg:{}".format(ret_orgin_data))
            return False, {}
        return True, ret_data


if __name__ == '__main__':
    handle = import_db()
    handle.run()

    print("exit sys...")