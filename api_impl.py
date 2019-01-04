import json
import os
from pymongo import MongoClient
from datetime import datetime, timedelta
import time

MONGO_HOST = os.environ.get('MONGO_HOST', '127.0.0.1')
MONGO_PORT = os.environ.get('MONGO_PORT', 27017)
MONGO_URL = "mongodb://{}".format(MONGO_HOST)

from open_explorer_api.web_request import client

Database = 'database'
Asset = 'asset'
History = 'history'


def get_header():
    api = 'database'
    method_fun = 'get_dynamic_global_properties'
    params = []
    result, ret = client.request(api, method_fun, params)
    if not result:
        return False, {}
    ret_requst = ret['result']

    method_fun = 'get_witness_count'
    result, ret_witness_count = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_requst['witness_count'] = ret_witness_count['result']

    method_fun = 'get_committee_count'
    result, ret_committee_count = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_requst['commitee_count'] = ret_committee_count['result']

    method_fun = 'get_objects'
    params = [['2.3.0']]
    result, ret_get_object = client.request(api, method_fun, params)
    if not result:
        return False, []
    current_supply = ret_get_object['result'][0]["current_supply"]
    confidental_supply = ret_get_object['result'][0]["confidential_supply"]
    market_cap = int(current_supply) + int(confidental_supply)
    ret_requst["bts_market_cap"] = int(market_cap / 100000000)

    # method_fun = 'get_24_volume'
    # params = []
    # result, ret_24_volume = client.request(api, method_fun, params)
    # if not result:
    #     return False, []

    ret_requst["quote_volume"] = 0  # ret_24_volume['result'][0]["quote_volume"]

    ret_data = sorted(ret_requst.items(), key=lambda d: d[0])
    return True, ret_data


def is_object(str_para):
    return len(str_para.split(".")) == 3


def get_holders():
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'holders'
    db_holders = db_client[table_base][table]
    try:
        count = db_holders.find().count()
        holders_data = list(db_holders.find().limit(count))
    except Exception:
        count = 0
        holders_data = []
    return count, holders_data


def get_market():
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'market'
    db_market = db_client[table_base][table]
    try:
        count = db_market.find().count()
        market_data = list(db_market.find().limit(count))
    except Exception:
        count = 0
        market_data = {}
    return count, market_data


def get_assert():
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'asset'
    db_asset = db_client[table_base][table]
    try:
        count = db_asset.find().count()
        assert_data = list(db_asset.find().limit(count))
    except Exception:
        count = 0
        assert_data = []
    return count, assert_data


def get_referrers():
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'referrers'
    db_referrers = db_client[table_base][table]
    try:
        count = db_referrers.find().count()
        referres_data = list(db_referrers.find().limit(count))
    except:
        count = 0
        referres_data = {}
    return count, referres_data


def get_operations():
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'operation'
    db_ops = db_client[table_base][table]
    try:
        count = db_ops.find().count()
        referres_data = list(db_ops.find().limit(count))
    except Exception:
        count = 0
        referres_data = {}
    return count, referres_data


def get_account(account_id):
    api = 'database'
    method_fun = 'get_accounts'
    params = [[account_id]]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    return True, ret_orgin_data['result']


def get_account_name(account_id):
    api = 'database'
    method_fun = 'get_accounts'
    params = [[account_id]]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result'][0]['name']
    return True, ret_data


def get_account_id(account_name):
    if not is_object(account_name):
        api = 'database'
        method_fun = 'lookup_account_names'
        params = [[account_name], 0]
        result, ret_orgin_data = client.request(api, method_fun, params)
        if not result:
            return False, []
        return True, ret_orgin_data['result'][0]['id']
    else:
        return True, account_name


def get_operation(operation_id):
    api = 'database'
    method_fun = 'get_objects'
    params = [[operation_id]]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_object = ret_orgin_data['result'][0]
    if not ret_object:
        return True, {}

    method_fun = 'get_dynamic_global_properties'
    params = []
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result']
    ret_object["accounts_registered_this_interval"] = ret_data['accounts_registered_this_interval']

    api = 'database'
    method_fun = 'get_objects'
    params = [['2.3.0']]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    current_supply = ret_orgin_data['result'][0]["current_supply"]
    confidental_supply = ret_orgin_data['result'][0]["confidential_supply"]
    market_cap = int(current_supply) + int(confidental_supply)
    ret_object["bts_market_cap"] = int(market_cap / 100000000)

    ret_result, global_properties = get_global_properties()
    if not ret_result:
        ret_object["commitee_count"] = 0
        ret_object["witness_count"] = 0
        return ret_result, ret_object
    ret_object["commitee_count"] = len(global_properties["active_committee_members"])
    ret_object["witness_count"] = len(global_properties["active_witnesses"])
    return True, ret_object


def get_global_properties():
    api = 'database'
    method_fun = 'get_global_properties'
    params = []
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result']
    return True, ret_data


def get_operation_full(operation_id):
    return get_operation(operation_id)


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
def get_operation_full_elastic(operation_id):
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'syndb'
    db_syn = db_client[table_base][table]
    count = db_syn.find({"operation_id": operation_id}).count()
    ret_trx_db = list(db_syn.find({"operation_id": operation_id}).limit(count))
    if count <= 0:
        return True,

    result, ret_op = get_operation(operation_id)
    if not result:
        return False, []
    ret_op['block_time'] = ret_trx_db[0]['datetime']
    ret_op['trx_id'] = ret_trx_db[0]['trx_id']
    return True, ret_op


def get_accounts():
    api = Asset
    method_fun = 'get_asset_holders'
    params = ['1.3.0', 0, 100]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result']
    return True, ret_data


def get_full_account(account_id):
    api = Database
    method_fun = 'get_full_accounts'
    params = [[account_id], 0]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result']
    return True, ret_data


def get_fees():
    return get_global_properties()


def get_account_history(account_id):
    if not is_object(account_id):
        api = Database
        method_fun = 'lookup_account_names'
        params = [[account_id], 0]
        result, ret_orgin_data = client.request(api, method_fun, params)
        if not result:
            return False, []
        account_id = ret_orgin_data['result'][0]['id']

    api = History
    method_fun = 'get_account_history'
    params = [account_id, '1.11.1', 20, '1.11.999999999']
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    account_history = ret_orgin_data['result']

    if len(account_history) > 0:
        for transaction in account_history:
            ret_result, ret_block_header = get_block_header(transaction['block_num'])
            if not ret_result:
                transaction["timestamp"] = 0
                transaction["witness"] = 0
                continue
            transaction["timestamp"] = ret_block_header["timestamp"]
            transaction["witness"] = ret_block_header["witness"]
    return True, account_history


def get_block_header(block_num):
    api = Database
    method_fun = 'get_block_header'
    params = [block_num, 0]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result']
    return True, ret_data


def get_block_asset(asset_id):
    api = Database
    method_fun = 'lookup_asset_symbols'
    params = [[asset_id], 0]

    if not is_object(asset_id):
        result, ret_orgin_data = client.request(api, method_fun, params)
        if not result:
            return False, []
    else:
        method_fun = 'get_assets'
        result, ret_orgin_data = client.request(api, method_fun, params)
        if not result:
            return False, []
    ret_list_account = ret_orgin_data['result'][0]

    api = 'database'
    method_fun = 'get_objects'
    params = [[ret_list_account["dynamic_asset_data_id"]]]
    result, dynamic_asset_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_list_account["current_supply"] = dynamic_asset_data['result'][0]["current_supply"]
    ret_list_account["confidential_supply"] = dynamic_asset_data['result'][0]["confidential_supply"]
    ret_list_account["accumulated_fees"] = dynamic_asset_data['result'][0]["accumulated_fees"]
    ret_list_account["fee_pool"] = dynamic_asset_data['result'][0]["fee_pool"]

    api = 'database'
    method_fun = 'get_objects'
    params = [[ret_list_account["issuer"]]]
    result, issuer = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_list_account["issuer_name"] = issuer['result'][0]["name"]
    ret_data = ret_list_account
    return True, ret_data


def get_asset(asset_id):
    return get_block_asset(asset_id)


def get_block(block_num):
    api = Database
    method_fun = 'get_block'
    params = [block_num, 0]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result']
    return True, ret_data


def get_last_network_ops():
    count, ret_ops_data = get_operations()
    if count <= 0:
        return True, []

    ret_data = sorted(ret_ops_data, key=lambda k: k['block_num'], reverse=True)
    ret_datas = []
    for i in range(count):
        ret_data[i].pop('_id')
        ret_datas.append('', ret_data[i]['oh'], ret_data[i]['ath'],
                         ret_data[i]['block_num'], ret_data[i]['trx_in_block'],
                         ret_data[i]['op_in_trx'], ret_data[i]['datetime'],
                         ret_data[i]['account_id'], ret_data[i]['op_type'], ret_data[i]['account_name'])
    return True, ret_datas


def get_object(object_id):
    api = Database
    method_fun = 'get_objects'
    params = [[object_id]]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result'][0]
    return True, ret_data


def get_asset_holders_count(asset_id):
    api = Asset
    method_fun = 'get_asset_holders_count'
    params = [asset_id]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    return True, ret_orgin_data['result']


def get_asset_holders(asset_id, start, limit):
    api = Asset
    method_fun = 'get_asset_holders'
    params = [asset_id, start, limit]
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    ret_data = ret_orgin_data['result']
    return True, ret_data


def get_workers():
    api = Database
    method_fun = 'get_worker_count'
    params = []
    result, ret_orgin_data = client.request(api, method_fun, params)
    if not result:
        return False, []
    workers_count = int(ret_orgin_data['result'])

    if workers_count <= 0:
        return False, []

    ret_objects = []
    for i in range(workers_count):
        api = Database
        method_fun = 'get_objects'
        params = [['1.14.{}'.format(i)]]
        result, ret_object = client.request(api, method_fun, params)
        if not result:
            continue
        if not ret_object['result'][0]:
            continue
        ret_objects.append(ret_object['result'][0])

    # get the votes of worker 1.14.0 - refund 400k
    ret_result, refund400k = get_object('1.14.0')
    if not ret_result:
        return ret_result, refund400k
    thereshold = int(refund400k["total_votes_for"])

    result = []
    for worker in ret_objects:
        if worker:
            api = Database
            method_fun = 'get_accounts'
            params = [worker["worker_account"]]
            result, ret_orgin_data = client.request(api, method_fun, params)
            if not result:
                continue
            worker["worker_account_name"] = ret_orgin_data['result'][0]['name']
            current_votes = int(worker["total_votes_for"])
            perc = (current_votes * 100) / thereshold
            worker["perc"] = perc
            result.append([worker])

    result = result[::-1]  # Reverse list.
    return True, result


def get_witnesses():
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'witness'
    db_witness = db_client[table_base][table]
    try:
        count = db_witness.find().count()
    except Exception:
        count = 0
    if count <= 0:
        return False, []

    ret_wit_db = list(db_witness.find().limit(count))
    for i in range(count):
        ret_wit_db[i].pop("_id")

    ret_wit_data = sorted(ret_wit_db, key=lambda k: k['total_votes'], reverse=True)
    return True, ret_wit_data


def get_committee_members():
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'committee'
    db_committee = db_client[table_base][table]
    try:
        count = db_committee.find().count()
    except Exception:
        count = 0
    if count <= 0:
        return False, []

    ret_comm_db = list(db_committee.find().limit(count))
    for i in range(count):
        ret_comm_db[i].pop("_id")

    ret_comm_data = sorted(ret_comm_db, key=lambda k: k['total_votes'], reverse=True)
    return True, ret_comm_data


def get_market_chart_data(base, quote):
    api = Database
    method_fun = 'lookup_asset_symbols'
    params = [[base], 0]
    ret_result, ret_base_data = client.request(api, method_fun, params)
    if not ret_result:
        return ret_result, []
    base_id = ret_base_data['result'][0]["id"]
    base_precision = 10 ** float(ret_base_data[0]["precision"])

    params = [[quote], 0]
    ret_result, ret_quote_data = client.request(api, method_fun, params)
    if not ret_result or ret_quote_data == [None]:
        return ret_result, []
    quote_id = ret_quote_data['result'][0]["id"]
    quote_precision = 10 ** float(ret_quote_data['result'][0]["precision"])

    api = History
    method_fun = 'get_market_history'
    now = datetime.date.today()
    ago = now - datetime.timedelta(days=100)
    params = [base_id, quote_id, 86400, ago.strftime("%Y-%m-%dT%H:%M:%S"), now.strftime("%Y-%m-%dT%H:%M:%S")]
    ret_result, market_history = client(api, method_fun, params)
    if not ret_result:
        return ret_result, []
    data = []
    for market_operation in market_history['result']:
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


def get_top_proxies():
    count, ret_holders_data = get_holders()
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


def get_formatted_proxy_votes(proxies, vote_id):
    return list(map(lambda p: '{}:{}'.format(p['id'], 'Y' if vote_id in p["options"]["votes"] else '-'), proxies))


def get_witnesses_votes():
    ret_result, top_proxies = get_top_proxies()
    if not ret_result:
        return ret_result, top_proxies
    top_proxies = top_proxies[:10]

    proxies = []
    for p in top_proxies:
        ret_result, proxes = get_object(p[0])
        if not ret_result:
            continue
        proxies.append(proxes)

    ret_result, witnesses = get_witnesses()
    if not ret_result:
        return ret_result, witnesses
    witnesses = witnesses[:25]  # FIXME: Witness number is variable.

    witnesses_votes = []
    for witness in witnesses:
        vote_id = witness["vote_id"]
        id_witness = witness["id"]
        witness_account_name = witness["witness_account_name"]
        proxy_votes = get_formatted_proxy_votes(proxies, vote_id)

        witnesses_votes.append([witness_account_name, id_witness] + proxy_votes)

    return True, witnesses_votes


def get_workers_votes():
    ret_result, top_proxies = get_top_proxies()
    if not ret_result:
        return ret_result, top_proxies

    top_proxies = top_proxies[:10]
    proxies = []
    for p in top_proxies:
        ret_result, proxes = get_object(p[0])
        if not ret_result:
            continue
        proxies.append(proxes)

    ret_result, workers = get_workers()
    if not ret_result:
        return ret_result, workers
    workers = workers[:30]

    workers_votes = []
    for worker in workers:
        vote_id = worker["vote_for"]
        id_worker = worker["id"]
        worker_account_name = worker["worker_account_name"]
        worker_name = worker["name"]
        proxy_votes = get_formatted_proxy_votes(proxies, vote_id)

        workers_votes.append([worker_account_name, id_worker, worker_name] + proxy_votes)

    return True, workers_votes


def get_committee_votes():
    ret_result, top_proxies = get_top_proxies()
    if not ret_result:
        return ret_result, top_proxies

    top_proxies = top_proxies[:10]
    proxies = []
    for p in top_proxies:
        ret_result, proxes = get_object(p[0])
        if not ret_result:
            continue
        proxies.append(proxes)

    ret_result, committee_members = get_committee_members()
    if not ret_result:
        return ret_result, committee_members
    committee_members = committee_members[:11]

    committee_votes = []
    for committee_member in committee_members:
        vote_id = committee_member["vote_id"]
        id_committee = committee_member["id"]
        committee_account_name = committee_member["committee_member_account_name"]
        proxy_votes = get_formatted_proxy_votes(proxies, vote_id)

        committee_votes.append([committee_account_name, id_committee] + proxy_votes)

    return True, committee_votes


def static_ops(list_ops, op_type):
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


def get_top_operations():
    count, ret_ops_data = get_operations()
    if count <= 0:
        return True, []

    ops = []
    for i in range(count):
        ops = static_ops(ops, ret_ops_data[i]['op_type'])

    ret_op = sorted(ops, key=lambda k: k[1], reverse=True)
    return True, ret_op[0]


def get_last_network_transactions():
    count, ret_ops_data = get_operations()
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


def lookup_accounts(start):
    api = Database
    method_fun = 'lookup_accounts'
    params = [start, 1000]
    result, accounts = client.request(api, method_fun, params)
    if not result:
        return result, []
    return True, accounts


def get_last_block_number():
    api = Database
    method_fun = 'get_dynamic_global_properties'
    params = ['']
    ret_result, dynamic_global_properties = client.request(api, method_fun, params)
    if not ret_result:
        return ret_result, dynamic_global_properties
    return True, dynamic_global_properties['result']["head_block_number"]


def get_account_history_pager(account_id, page):
    ret_result, account_id = get_account_id(account_id)
    if not ret_result:
        return ret_result, account_id

    # need to get total ops for account
    api = Database
    method_fun = 'get_accounts'
    params = [[account_id]]
    ret_result, ret_account = client.request(api, method_fun, params)
    if not ret_result or ret_account == [None]:
        return ret_result, ret_account
    account = ret_account['result'][0]

    ret_result, statistics = get_object(account["statistics"])
    if not ret_result:
        return ret_result, statistics

    total_ops = statistics["total_ops"]
    start = total_ops - (20 * int(page))
    stop = total_ops - (40 * int(page))

    if stop < 0:
        stop = 0

    if start > 0:
        api = History
        method_fun = 'get_relative_account_history'
        params = [account_id, stop, 20, start]
        ret_result, account_history = client.request(api, method_fun, params)
        if not ret_result:
            return ret_result, account_history

        for transaction in account_history['result']:
            ret_result, block_header = get_block_header(transaction["block_num"])
            if not ret_result:
                continue
            transaction["timestamp"] = block_header["timestamp"]
            transaction["witness"] = block_header["witness"]
        return True, account_history['result']
    else:
        return False, []


def get_operation_history(his_info):
    op_info = his_info["op"]
    operation_history = {"op": json.dumps(op_info), "op_in_trx": his_info["op_in_trx"], "operation_result": str(his_info["result"]),
                         "trx_in_block": his_info["trx_in_block"], "virtual_op": his_info["virtual_op"]}
    return True, operation_history


def get_account_history_pager_elastic(account_id, page):
    db_holders_count, ret_db_holders = get_holders()
    if db_holders_count <= 0:
        return False, []

    for i in range(db_holders_count):
        if account_id == ret_db_holders[i]['account_name']:
            account_id = ret_db_holders[i]['account_id']
            break
        if account_id == ret_db_holders[i]['account_id']:
            break

    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'syndb'
    db_syn = db_client[table_base][table]
    count = db_syn.find({"account_id": account_id}).sort("id_index", -1).count()
    if count <= 0:
        return False, []
    page_count = int(page) * 20
    if page_count < 3000:
        count = 3000
    else:
        if page_count < count:
            count = page_count
        else:
            return True, []

    ret_db = list(db_syn.find({"account_id": account_id}).sort("id_index", -1).limit(count))

    ret_filter_data = []
    ret_sort_db = sorted(ret_db, key=lambda k:k["id_index"], reverse=True)
    for i in range(len(ret_sort_db) - 1):
        if ret_sort_db[i]["operation_id"] != ret_sort_db[i + 1]["operation_id"]:
            ret_filter_data.append(ret_sort_db[i])
        else:
            continue

    ret_filter_data = sorted(ret_filter_data, key=lambda k:k['timestamp'], reverse=True)
    ret_filter_data = ret_filter_data[page_count:page_count + min(20, count - page_count)]
    results = []
    for i in range(len(ret_filter_data)):
        block_num = ret_filter_data[i]["block_num"]
        id = ret_filter_data[i]["operation_id"]
        timestamp = ret_filter_data[i]["datetime"]

        ret_result, ret_objects_op = get_object(id)
        if not ret_result or ret_objects_op == None:
            continue

        ret_result, operation_history = get_operation_history(ret_objects_op)
        operation_history['op'] = ret_objects_op['op']
        operation_history["block_num"] = block_num
        operation_history["id"] = id
        operation_history["timestamp"] = timestamp
        results.append(operation_history)
    return True, results


def get_daily_volume_dex_dates():
    base = datetime.date.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, 60)]
    date_list = [d.strftime("%Y-%m-%d") for d in date_list]
    return True, list(reversed(date_list))


def get_daily_volume_dex_data():
    # todo:需要按时间排序，现在没有时间这个字段
    count, ret_asset_db = get_assert()
    if count <= 0:
        return True, []

    dex_data = []
    for i in range(count):
        if ret_asset_db[i]['aname'] != 'BTS':
            dex_data.append(ret_asset_db[i]['volume'])

    return True, dex_data


def ensure_asset_id(asset_id):
    api = Database
    method_fun = 'lookup_asset_symbols'
    params = [[asset_id], 0]
    ret_result, ret_orgin_data = client.request(api, method_fun, params)
    if not ret_result:
        return ret_result, []
    ret_list_account = ret_orgin_data['result'][0]
    return True, ret_list_account['id']


def get_all_asset_holders(asset_id):
    ret_result, asset_id = ensure_asset_id(asset_id)

    all = []

    ret_result, asset_holders = get_asset_holders(asset_id, 0, 100)
    if not ret_result:
        return ret_result, asset_holders

    all.extend(asset_holders)

    len_result = len(asset_holders)
    start = 100
    while len_result == 100:
        start = start + 100
        ret_result, asset_holders = get_asset_holders(asset_id, start, 100)
        if not ret_result:
            continue
        len_result = len(asset_holders)
        all.extend(asset_holders)

    return True, all


def get_referrer_count(account_id):
    ret_result, account_id = get_account_id(account_id)
    if not ret_result:
        return ret_result, account_id

    count, ret_ref_data = get_referrers()
    if count <= 0:
        return True, 0

    results = 0
    for i in range(count):
        if ret_ref_data[i]['referrer'] == account_id:
            results += 1
    return True, results


def get_all_referrers(account_id, page=0):
    ret_result, account_id = get_account_id(account_id)
    if not ret_result:
        return ret_result, account_id

    count, ret_ref_data = get_referrers()
    if count <= 0:
        return True, 0

    ret_ref_data.reverse()

    ref_datas = []
    for i in range(count):
        if ret_ref_data[i]['referrer'] == account_id:
            ret_ref_data[i].pop('_id')
            ret_ref_data[i]['db_index'] = i
            ref_datas.append(ret_ref_data[i])

    offset = int(page) * 20
    if offset > len(ref_datas):
        return True, ref_datas
    else:
        results = []
        if offset > len(ref_datas):
            return True, []
        else:
            count = min(len(ref_datas) - offset, 20)
            for i in range(count):
                result = [ref_datas[offset + i]['db_index'], ref_datas[offset + i]['account_id'], ref_datas[offset + i]['account_name'],
                           ref_datas[offset + i]['lifetime_referrer'],
                           ref_datas[offset + i]['lifetime_referrer_fee_percentage'],
                           ref_datas[offset + i]['referrer'], ref_datas[offset + i]['referrer_rewards_percentage']]
                results.append(result)
            return True, results

def get_syn_data():
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'syndb'
    db_syn = db_client[table_base][table]
    try:
        count = db_syn.find().sort("block_num", -1).count()
        if count > 3000:
            count = 3000
        referres_data = list(db_syn.find().sort("block_num", -1).limit(count))
    except:
        count = 0
        referres_data = {}
    for i in range(count):
        referres_data[i].pop("timestamp")
    return count, referres_data


def get_additional_data(account_id, account_info):
    op_info = account_info[1]
    fee_data = dict(amount=op_info["fee"]["amount"], asset=op_info["fee"]["asset_id"])

    # todo :hardcode 需要改进
    account_id = account_id
    if "order" in op_info:
        order_id = op_info["order"]
    else:
        order_id = '0.0.0'
    if "is_make" in op_info:
        is_maker = op_info["is_maker"]
    else:
        is_maker = "true"
    if "pays" in op_info:
        pays_amount = op_info["pays"]["amount"]
        pays_asset_id = op_info["pays"]["asset_id"]
        receives_amount = op_info["receives"]["amount"]
        receives_asset_id = op_info["receives"]["receives_asset_id"]
    else:
        pays_amount = 0
        pays_asset_id = "1.3.0"
        receives_amount = 0
        receives_asset_id = "1.3.0"

    if "fill_price" in op_info:
        fill_price = float(op_info["fill_price"]["base"]["amount"]) / float(
            op_info["fill_price"]["quote"]["amount"])
    else:
        fill_price = 0

    fill_data = dict(account_id=account_id, fill_price=fill_price, is_maker=is_maker, order_id=order_id,
                     pays_amount=pays_amount, pays_asset_id=pays_asset_id,
                     receives_amount=receives_amount, receives_asset_id=receives_asset_id)
    # transfer_data = {'amount': op_info['amount']['amount'], 'asset': op_info['amount']['asset_id'],
    #                  'from': op_info['from'], 'to': op_info['to']}
    transfer_data = {"amount": 0, "asset": "1.3.0", "from": "1.2.0", "to": "1.2.0"}  # todo:从explorer返回的数据像硬编码，后面再改

    addtional_data = {"fee_data": fee_data, "fill_data": fill_data, "transfer_data": transfer_data}
    return True, addtional_data


def get_operation_history(his_info):
    op_info = his_info["op"]
    operation_history = {"op": json.dumps(op_info), "op_in_trx": his_info["op_in_trx"], "operation_result": str(his_info["result"]),
                         "trx_in_block": his_info["trx_in_block"], "virtual_op": his_info["virtual_op"]}
    return True, operation_history


def get_account_history_elastic(size, start):
    count, ret_ops_data = get_syn_data()
    if count <= 0:
        return False, []

    ret_filter_data = []
    ret_op_sort = sorted(ret_ops_data, key=lambda k: k['block_num'], reverse=True)
    for i in range(len(ret_op_sort) - 1):
        if ret_op_sort[i]["operation_id"] != ret_op_sort[i + 1]["operation_id"]:
            ret_filter_data.append(ret_op_sort[i])
        else:
            continue

    ret_opts_sort = sorted(ret_filter_data, key=lambda k: k['block_num'], reverse=True)

    ret_opts_sort = ret_opts_sort[start:(start + size)]
    history_elastic = []
    for i in range(len(ret_opts_sort)):
        op_id = ret_opts_sort[i]['oh']
        ath = ret_opts_sort[i]['operation_id']
        block_num = ret_opts_sort[i]['block_num']
        account_id = ret_opts_sort[i]['account_id']
        ret_result, ret_objects_op = get_object(ath)
        if not ret_result or ret_objects_op == None:
            continue

        ret_his_op = ret_objects_op
        block_info = {
            'block_num': block_num, 'block_time': ret_opts_sort[i]['datetime'],
            'trx_id': ret_opts_sort[i]['trx_id']
        }

        account_history_info = {
            'account': account_id, 'id': op_id, 'next': ret_opts_sort[i]['next'],
            'operation_id': ath, 'sequence': ret_opts_sort[i]['sequence']}

        ret_result, additional_data = get_additional_data(account_id, ret_his_op['op'])
        if not ret_result:
            continue

        ret_result, operation_history = get_operation_history(ret_his_op)
        if not ret_result:
            continue

        operation_id_num = int(op_id.split('.')[2])

        dict_tmp = {'account_history': account_history_info, 'additional_data': additional_data,
                    'block_data': block_info, 'operation_history': operation_history,
                    'operation_id_num': operation_id_num, 'operation_type': ret_his_op['op'][0]}

        history_elastic.append(dict_tmp)
    return True, history_elastic


def get_account_history_elastic2(from_date, to_date, type, agg_field, size):
    # 取size 个
    # print("get_account_history_elastic2, stime:{}".format(time.time()))
    days = from_date.split('-')[1]
    period = days[-1]
    days = days[0:(len(days) - 1)]

    if period == 'h':
        start_time = (datetime.now() - timedelta(seconds=(3600 * int(days)))).strftime("%Y-%m-%dT%H:%M:%S")
    elif period == 'd':
        start_time = (datetime.now() - timedelta(days=int(days))).strftime("%Y-%m-%dT%H:%M:%S")
    elif period == 'w':
        start_time = (datetime.now() - timedelta(days=7 * int(days))).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        start_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    start_time = int(datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").timestamp())
    end_time = int(datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S").timestamp())

    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'syndb'
    db_syn = db_client[table_base][table]

    # count = self.__db_syn.find({'timestamp': {'$gte': start_time, '$lte': end_time}}).count()
    count = db_syn.find().sort('block_num', -1).count()
    if count <= 0:
        return True, []
    if count > 3000:
        count = 3000
    # print("get_account_history_elastic2,count:{}, time:{}".format(count, int(time.time())))
    ret_syn_db = list(
        db_syn.find().sort('block_num', -1).limit(count))
    list_ops = []
    for i in range(len(ret_syn_db)):
        list_ops = static_ops(list_ops, ret_syn_db[i]['block_num'])

    list_ops = []
    for i in range(len(ret_syn_db)):
        list_ops = static_ops(list_ops, ret_syn_db[i]['op_type'])
    ret_data = []
    for i in range(len(list_ops)):
        ret_data.append({'doc_count': list_ops[i][1], 'key':list_ops[i][0]})

    ret_sort_data = sorted(ret_data, key=lambda k: k['doc_count'], reverse=True)
    ret_sort_data = ret_sort_data[0:min(size, len(ret_sort_data))]
    # print("get_account_history_elastic2, etime:{}".format(time.time()))
    return True, ret_sort_data

def get_account_history_elastic3(from_date, to_date, type, agg_field, size):
    days = from_date.split('-')[1]
    period = days[-1]
    days = days[0:(len(days) - 1)]

    if period == 'h':
        start_time = (datetime.now() - timedelta(seconds=(3600 * int(days)))).strftime("%Y-%m-%dT%H:%M:%S")
    elif period == 'd':
        start_time = (datetime.now() - timedelta(days=int(days))).strftime("%Y-%m-%dT%H:%M:%S")
    elif period == 'w':
        start_time = (datetime.now() - timedelta(days=7 * int(days))).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        start_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    start_time = int(datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").timestamp())
    end_time = int(datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S").timestamp())

    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'syndb'
    db_syn = db_client[table_base][table]
    count = db_syn.find().sort('block_num', -1).count()
    if count <= 0:
        return True, []

    if count > 3000:
        count = 3000
    ret_syn_db = list(
        db_syn.find().sort('block_num', -1).limit(count))
    list_ops = []
    for i in range(len(ret_syn_db)):
        list_ops = static_ops(list_ops, ret_syn_db[i]['block_num'])

    ret_data = []
    for i in range(len(list_ops)):
        ret_data.append({'doc_count': list_ops[i][1], 'key': list_ops[i][0]})
    ret_sort_data = sorted(ret_data, key=lambda k: k['doc_count'], reverse=True)
    ret_sort_data = ret_sort_data[0:min(size, len(ret_sort_data))]
    return True, ret_sort_data


def get_account_history_elastic4(from_date, to_date, type, agg_field, size):
    # print("get_account_history_elastic4, stime:{}".format(time.time()))
    days = from_date.split('-')[1]
    period = days[-1]
    days = days[0:(len(days) - 1)]

    if period == 'h':
        start_time = (datetime.now() - timedelta(seconds=(3600 * int(days)))).strftime("%Y-%m-%dT%H:%M:%S")
    elif period == 'd':
        start_time = (datetime.now() - timedelta(days=int(days))).strftime("%Y-%m-%dT%H:%M:%S")
    elif period == 'w':
        start_time = (datetime.now() - timedelta(days=7 * int(days))).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        start_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    start_time = int(datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").timestamp())
    end_time = int(datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S").timestamp())

    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'syndb'
    db_syn = db_client[table_base][table]
    count = db_syn.find().sort('block_num', -1).count()
    if count <= 0:
        return True, []
    if count > 3000:
        count = 3000
    # print("get_account_history_elastic4,count:{}, time:{}".format(count, int(time.time())))
    ret_syn_db = list(
        db_syn.find().sort('block_num', -1).limit(
            count))

    ret_filter_data = []
    ret_opts_sort = sorted(ret_syn_db, key=lambda k: k['id_index'], reverse=True)
    for i in range(len(ret_opts_sort) - 1):
        if ret_opts_sort[i]['operation_id'] != ret_opts_sort[i + 1]['operation_id']:
            ret_filter_data.append(ret_opts_sort[i])
        else:
            continue

    list_ops = []
    ret_syn_db = sorted(ret_filter_data, key=lambda k: k['block_num'], reverse=True)
    for i in range(len(ret_syn_db)):
        list_ops = static_ops(list_ops, ret_syn_db[i]['trx_id'])

    ret_data = []
    for i in range(len(list_ops)):
        ret_data.append({'doc_count': list_ops[i][1], 'key': list_ops[i][0]})

    ret_sort_data = sorted(ret_data, key=lambda k: k['doc_count'], reverse=True)
    ret_sort_data = ret_sort_data[0:min(size, len(ret_sort_data))]
    # print("get_account_history_elastic4, etime:{}".format(time.time()))
    return True, ret_sort_data


def get_trx(trx_id, size):
    db_client = MongoClient(MONGO_URL + '/?authSource=admin', MONGO_PORT, connect=False)
    table_base = 'explorer'
    table = 'syndb'
    db_syn = db_client[table_base][table]
    count = db_syn.find({"trx_id":trx_id}).count()
    ret_trx_db = list(db_syn.find({"trx_id":trx_id}).limit(count))
    if count <= 0:
        return True,
    # print("get_trx2, count:{}".format(count))
    ret_trx_data = sorted(ret_trx_db, key=lambda k:k['id_index'], reverse=True)
    ret_filter_data = []
    for i in range(len(ret_trx_data) - 1):
        if ret_trx_data[i]['operation_id'] == ret_trx_data[i + 1]['operation_id']:
            ret_filter_data.append(ret_trx_data[i])
        else:
            continue

    ret_opts_sort = sorted(ret_filter_data, key=lambda k: k['block_num'], reverse=True)
    history_elastic = []
    for i in range(len(ret_opts_sort)):
        if ret_opts_sort[i]['trx_id'] == trx_id:
            op_id = ret_opts_sort[i]['oh']
            ath = ret_opts_sort[i]['operation_id']
            block_num = ret_opts_sort[i]['block_num']
            account_id = ret_opts_sort[i]['account_id']
            ret_result, ret_objects_op = get_object(ath)
            if not ret_result:
                continue

            ret_his_op = ret_objects_op
            block_info = {
                'block_num': block_num, 'block_time': ret_opts_sort[i]['datetime'],
                'trx_id': ret_opts_sort[i]['trx_id']
            }

            account_history_info = {'account': account_id, 'id': op_id, 'next': ret_opts_sort[i]['next'],
                                    'operation_id': ath, 'sequence': ret_opts_sort[i]['sequence']}

            ret_result, additional_data = get_additional_data(account_id, ret_his_op['op'])
            if not ret_result:
                continue

            ret_result, operation_history = get_operation_history(ret_his_op)
            if not ret_result:
                continue

            operation_id_num = int(op_id.split('.')[2])

            dict_tmp = {'account_history': account_history_info, 'additional_data': additional_data,
                        'block_data': block_info, 'operation_history': operation_history,
                        'operation_id_num': operation_id_num, 'operation_type': ret_his_op['op'][0]}

            history_elastic.append(dict_tmp)
    ret_trx_sorted = history_elastic[:min(size, len(history_elastic))]
    return True, ret_trx_sorted
