import threading

from flask import Flask, jsonify, make_response
from flask import request
app = Flask(__name__)

import open_explorer_api.api_impl as impl_handle
from open_explorer_api.get_blockchain_data import import_db


@app.route('/header', methods=['GET', 'POST'])
def header():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_header()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(dict(ret_data)))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst

@app.route('/top_proxies', methods=['GET', 'POST'])
def top_proxies():
    header_origin = request.headers['Origin']
    count, ret_holders_data = impl_handle.get_holders()
    ret_data_tmp = []
    ret_data = []
    total_amount = 0
    ret_request = []
    if count > 0:
        for i in range(count):
            ret_data_tmp.append(ret_holders_data[i]['account_id'])
            ret_data_tmp.append(ret_holders_data[i]['account_name'])
            ret_data_tmp.append(float(ret_holders_data[i]['amount']))
            ret_data_tmp.append(ret_holders_data[i]['vote_account'])
            ret_data_tmp.append(float(ret_holders_data[i]['holder_reserve']))
            ret_data.append(ret_data_tmp)
            ret_data_tmp = []
            total_amount += float(ret_holders_data[i]['amount'])

        for i in range(len(ret_data)):
            ret_data[i][4] = float(ret_data[i][2] * 100 / total_amount)
        ret_request = sorted(ret_data, key=lambda k: -k[2])
    rst = make_response(jsonify(ret_request))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/top_holders', methods=['GET', 'POST'])
def top_holders():
    header_origin = request.headers['Origin']
    count, ret_holders_data = impl_handle.get_holders()
    ret_data_tmp = []
    ret_data = []
    ret_request = []
    if count > 0:
        for i in range(count):
            if float(ret_holders_data[i]['amount']) <= 0:
                continue
            ret_data_tmp.append(float(ret_holders_data[i]['holder_reserve']))
            ret_data_tmp.append(ret_holders_data[i]['account_id'])
            ret_data_tmp.append(ret_holders_data[i]['account_name'])
            ret_data_tmp.append(float(ret_holders_data[i]['amount']))
            ret_data_tmp.append(ret_holders_data[i]['vote_account'])
            ret_data.append(ret_data_tmp)
            ret_data_tmp = []
        ret_request = sorted(ret_data, key=lambda k: k[3], reverse=True)
    rst = make_response(jsonify(ret_request))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/top_markets', methods=['GET', 'POST'])
#todo :这个接口可以考虑删除
def top_markets():
    header_origin = request.headers['Origin']
    count, ret_market_data = impl_handle.get_market()

    ret_request = []
    if count > 0:
        market_data = sorted(ret_market_data, key=lambda k: k['volume'], reverse=True)
        for i in range(min(count, 7)):
            ret_request_tmp = [market_data[i]['asset_symbol'] + '/' + market_data[i]['symbol'],
                               market_data[i]['volume']]
            ret_request.append(ret_request_tmp)

    rst = make_response(jsonify(ret_request))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/top_smartcoins', methods=['GET', 'POST'])
def top_smartcoins():
    header_origin = request.headers['Origin']

    count, ret_smart_data = impl_handle.get_assert()
    ret_request = []
    if count > 0:
        market_data = sorted(ret_smart_data, key=lambda k: k['volume'], reverse=True)
        for i in range(min(count, 7)):
            if market_data[i]['type'] == 'SmartCoin':
                ret_request_tmp = [market_data[i]['aname'], market_data[i]['volume']]
                ret_request.append(ret_request_tmp)

    rst = make_response(jsonify(ret_request))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/top_uias', methods=['GET', 'POST'])
def top_uias():
    header_origin = request.headers['Origin']
    count, ret_uias_data = impl_handle.get_assert()
    ret_request = []
    if count > 0:
        market_data = sorted(ret_uias_data, key=lambda k: k['volume'], reverse=True)
        for i in range(min(count, 7)):
            if market_data[i]['type'] == 'User Issued':
                ret_request_tmp = [market_data[i]['aname'], market_data[i]['volume']]
                ret_request.append(ret_request_tmp)

    rst = make_response(jsonify(ret_request))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/account', methods=['GET', 'POST'])
def get_account():
    header_origin = request.headers['Origin']
    account_id = request.args.get('account_id')

    ret_result, ret_account_data = impl_handle.get_account(account_id)
    if not ret_result:
        ret_account_data = []

    rst = make_response(jsonify(dict(ret_account_data[0])))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/account_name', methods=['GET', 'POST'])
def get_account_name():
    header_origin = request.headers['Origin']
    account_id = request.args.get('account_id')

    ret_result, ret_account_data = impl_handle.get_account_name(account_id)
    if not ret_result:
        ret_account_data = []

    rst = make_response(jsonify(ret_account_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/account_id', methods=['GET', 'POST'])
def get_account_id():
    header_origin = request.headers['Origin']
    account_name = request.args.get('account_name')

    ret_result, ret_account_data = impl_handle.get_account_id(account_name)
    if not ret_result:
        ret_account_data = []

    rst = make_response(jsonify(ret_account_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/operation', methods=['GET', 'POST'])
def get_operation():
    header_origin = request.headers['Origin']
    operation_id = request.args.get('operation_id')

    ret_result, ret_operation_data = impl_handle.get_operation(operation_id)
    if not ret_result:
        ret_operation_data = []

    rst = make_response(jsonify(ret_operation_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/operation_full', methods=['GET', 'POST'])
def get_operation_full():
    header_origin = request.headers['Origin']
    operation_id = request.args.get('operation_id')

    ret_result, ret_operation_data = impl_handle.get_operation_full(operation_id)
    if not ret_result:
        ret_operation_data = []

    rst = make_response(jsonify(ret_operation_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/operation_full_elastic', methods=['GET', 'POST'])
def get_operation_full_elastic():
    header_origin = request.headers['Origin']
    operation_id = request.args.get('operation_id')

    ret_result, ret_operation_data = impl_handle.get_operation_full_elastic(operation_id)
    if not ret_result:
        ret_operation_data = []

    rst = make_response(jsonify([ret_operation_data]))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/accounts', methods=['GET', 'POST'])
def get_accounts():
    header_origin = request.headers['Origin']

    ret_result, ret_account_data = impl_handle.get_accounts()
    if not ret_result:
        ret_account_data = []

    rst = make_response(jsonify(ret_account_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/full_account', methods=['GET', 'POST'])
def get_full_account():
    header_origin = request.headers['Origin']
    account_id = request.args.get('account_id')

    ret_result, ret_account_data = impl_handle.get_full_account(account_id)
    if not ret_result:
        ret_account_data = []

    rst = make_response(jsonify(ret_account_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/assets', methods=['GET', 'POST'])
def get_assets():
    header_origin = request.headers['Origin']
    count, ret_asset_data = impl_handle.get_assert()
    ret_datas = []
    if count > 0:
        for i in range(count):
            ret_data = ['', ret_asset_data[i]['aname'], ret_asset_data[i]['aid'],
                        ret_asset_data[i]['price'], ret_asset_data[i]['volume'], float(ret_asset_data[i]['mcap']),
                        ret_asset_data[i]['type'], int(ret_asset_data[i]['current_supply']), int(ret_asset_data[i]['holders']),
                        ret_asset_data[i]['wallettype'], ret_asset_data[i]['precision']]
            ret_datas.append(ret_data)
        ret_sort = sorted(ret_datas, key=lambda k: k[4], reverse=True)
    else:
        ret_sort = []
    rst = make_response(jsonify(ret_sort))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/fees', methods=['GET', 'POST'])
def get_fees():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_fees()
    if not ret_result:
        ret_data = []

    rst = make_response(jsonify(dict(ret_data)))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/account_history', methods=['GET', 'POST'])
def get_account_history():
    header_origin = request.headers['Origin']
    account_id = request.args.get('account_id')
    ret_result, ret_data = impl_handle.get_account_history(account_id)
    if not ret_result:
        ret_data = []

    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_asset', methods=['GET', 'POST'])
def get_asset():
    header_origin = request.headers['Origin']
    asset_id = request.args.get('asset_id')
    ret_result, ret_data = impl_handle.get_asset(asset_id)
    if not ret_result:
        ret_data = []

    rst = make_response(jsonify([ret_data]))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_asset_and_volume', methods=['GET', 'POST'])
def get_asset_and_volume():
    header_origin = request.headers['Origin']
    asset_id = request.args.get('asset_id')
    ret_result, ret_get_data = impl_handle.get_asset(asset_id)
    count, ret_asset_data = impl_handle.get_assert()
    if count > 0 and ret_result:
        for i in range(count):
            if asset_id == ret_asset_data[i]['aid']:
                ret_get_data['volume'] = ret_asset_data[i]['volume']
                ret_get_data['mcap'] = ret_asset_data[i]['mcap']

    else:
        ret_get_data = []
    rst = make_response(jsonify([ret_get_data]))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/block_header', methods=['GET', 'POST'])
def get_block_header():
    header_origin = request.headers['Origin']
    block_num = request.args.get('block_num')
    ret_result, ret_data = impl_handle.get_block_header(block_num)
    if not ret_result:
        ret_data = []

    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_block', methods=['GET', 'POST'])
def get_block():
    header_origin = request.headers['Origin']
    block_num = request.args.get('block_num')
    ret_result, ret_data = impl_handle.get_block(block_num)
    if not ret_result:
        ret_data = []

    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_ticker', methods=['GET', 'POST'])
# todo:未来删除这个接口
def get_ticker():
    header_origin = request.headers['Origin']
    base = request.args.get('base')
    quote = request.args.get('quote')
    # ret_result, ret_data = impl_handle.get_ticker(base, quote)
    # if not ret_result:
    ret_data = []

    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_volume', methods=['GET', 'POST'])
def get_volume():
    header_origin = request.headers['Origin']
    # base = request.args.get('base')
    # quote = request.args.get('quote')
    # ret_result, ret_data = impl_handle.get_volume(base, quote)
    # if not ret_result:
    ret_data = []

    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/lastnetworkops', methods=['GET', 'POST'])
def get_last_network_ops():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_last_network_ops()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_object', methods=['GET', 'POST'])
def get_object():
    header_origin = request.headers['Origin']
    object_id = request.args.get('object')
    ret_result, ret_data = impl_handle.get_object(object_id)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify([ret_data]))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_asset_holders_count', methods=['GET', 'POST'])
def get_asset_holders_count():
    header_origin = request.headers['Origin']
    asset_id = request.args.get('asset_id')
    ret_result, ret_data = impl_handle.get_asset_holders_count(asset_id)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_asset_holders', methods=['GET', 'POST'])
def get_asset_holders():
    header_origin = request.headers['Origin']
    asset_id = request.args.get('asset_id')
    start = request.args.get('start')
    limit = request.args.get('limit')
    ret_result, ret_data = impl_handle.get_asset_holders(asset_id, start, limit)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_workers', methods=['GET', 'POST'])
def get_workers():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_workers()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_markets', methods=['GET', 'POST'])
def get_markets():
    header_origin = request.headers['Origin']
    asset_id = request.args.get('asset_id')
    count, ret_market_data = impl_handle.get_market()

    ret_data = []
    if count > 0:
        for i in range(count):
            if asset_id == ret_market_data[i]['aid']:
                ret_data_tmp = [i+1, ret_market_data[i]['asset_symbol'] + '/' + ret_market_data[i]['symbol'],
                                ret_market_data[i]['asset_id'], ret_market_data[i]['price'],
                                ret_market_data[i]['volume'], ret_market_data[i]['aid']]
                ret_data.append(ret_data_tmp)
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_most_active_markets', methods=['GET', 'POST'])
def get_most_active_markets():
    header_origin = request.headers['Origin']
    count, ret_market_data = impl_handle.get_market()

    ret_data = []
    if count > 0:
        for i in range(min(100, count)):
            if ret_market_data[i]['volume'] >= 0:
                ret_data_tmp = [i+1,ret_market_data[i]['asset_symbol'] + '/' + ret_market_data[i]['symbol'],
                                ret_market_data[i]['asset_id'], ret_market_data[i]['price'],
                                ret_market_data[i]['volume'], ret_market_data[i]['aid']]
                ret_data.append(ret_data_tmp)
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_order_book', methods=['GET', 'POST'])
#todo 这个接口要删除
def get_order_book():
    header_origin = request.headers['Origin']
    # base = request.args.get('base')
    # quote = request.args.get('quote')
    # limit = request.args.get('limit')
    # ret_result, ret_data = impl_handle.get_order_book(base, quote, limit)
    # if not ret_result:
    ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_margin_positions', methods=['GET', 'POST'])
#todo 这个接口要删除
def get_margin_positions():
    header_origin = request.headers['Origin']
    # account_id = request.args.get('account_id')
    # ret_result, ret_data = impl_handle.get_margin_positions(account_id)
    # if not ret_result:
    ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_witnesses', methods=['GET', 'POST'])
def get_witnesses():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_witnesses()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_committee_members', methods=['GET', 'POST'])
def get_committee_members():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_committee_members()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/market_chart_dates', methods=['GET', 'POST'])
#todo 可以删除
def get_market_chart_dates():
    header_origin = request.headers['Origin']
    # ret_result, ret_data = impl_handle.get_market_chart_dates()
    # if not ret_result:
    ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/market_chart_data', methods=['GET', 'POST'])
#todo 暂时保留
def get_market_chart_data():
    header_origin = request.headers['Origin']
    base = request.args.get('base')
    quote = request.args.get('quote')
    ret_result, ret_data = impl_handle.get_market_chart_data(base, quote)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/witnesses_votes', methods=['GET', 'POST'])
def get_witnesses_votes():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_witnesses_votes()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/workers_votes', methods=['GET', 'POST'])
def get_workers_votes():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_workers_votes()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/committee_votes', methods=['GET', 'POST'])
def get_committee_votes():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_committee_votes()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/top_operations', methods=['GET', 'POST'])
def get_top_operations():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_top_operations()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify([ret_data]))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/last_network_transactions', methods=['GET', 'POST'])
def get_last_network_transactions():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_last_network_transactions()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/lookup_accounts', methods=['GET', 'POST'])
def lookup_accounts():
    header_origin = request.headers['Origin']
    start = request.args.get('start')
    ret_result, ret_data = impl_handle.lookup_accounts(start)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/lookup_assets', methods=['GET', 'POST'])
def lookup_assets():
    header_origin = request.headers['Origin']
    start = request.args.get('start')
    count, ret_asset_data = impl_handle.get_assert()
    ret_data = []
    if count <= 0:
        ret_data = []
    else:
        for i in range(count):
            if start <= ret_asset_data[i]['aname'][0]:
                # ret_asset_data[i].pop('_id')
                ret_data.append(ret_asset_data[i]['aname'])
    rst = make_response(jsonify([ret_data]))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/getlastblocknumber', methods=['GET', 'POST'])
def get_last_block_number():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_last_block_number()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/account_history_pager', methods=['GET', 'POST'])
def get_account_history_pager():
    header_origin = request.headers['Origin']
    account_id = request.args.get('account_id')
    page = request.args.get('page')
    ret_result, ret_data = impl_handle.get_account_history_pager(account_id, page)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/account_history_pager_elastic', methods=['GET', 'POST'])
def get_account_history_pager_elastic():
    header_origin = request.headers['Origin']
    account_id = request.args.get('account_id')
    page = request.args.get('page')
    ret_result, ret_data = impl_handle.get_account_history_pager_elastic(account_id, page)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_limit_orders', methods=['GET', 'POST'])
#todo 可以删除
def get_limit_orders():
    header_origin = request.headers['Origin']
    # base = request.args.get('base')
    # quote = request.args.get('quote')
    # ret_result, ret_data = impl_handle.get_limit_orders(base, quote)
    # if not ret_result:
    ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_call_orders', methods=['GET', 'POST'])
#todo 可以删除
def get_call_orders():
    header_origin = request.headers['Origin']
    # asset_id = request.args.get('asset_id')
    # ret_result, ret_data = impl_handle.get_call_orders(asset_id)
    # if not ret_result:
    ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_settle_orders', methods=['GET', 'POST'])
#todo 可以删除
def get_settle_orders():
    header_origin = request.headers['Origin']
    # base = request.args.get('base')
    # ret_result, ret_data = impl_handle.get_settle_orders(base)
    # if not ret_result:
    ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_dex_total_volume', methods=['GET', 'POST'])
#todo 可以删除
def get_dex_total_volume():
    header_origin = request.headers['Origin']
    # ret_result, ret_data = impl_handle.get_dex_total_volume()
    # if not ret_result:
    ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/daily_volume_dex_dates', methods=['GET', 'POST'])
def get_daily_volume_dex_dates():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_daily_volume_dex_dates()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/daily_volume_dex_data', methods=['GET', 'POST'])
def get_daily_volume_dex_data():
    header_origin = request.headers['Origin']
    ret_result, ret_data = impl_handle.get_daily_volume_dex_data()
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_all_asset_holders', methods=['GET', 'POST'])
def get_all_asset_holders():
    header_origin = request.headers['Origin']
    asset_id = request.args.get('asset_id')
    ret_result, ret_data = impl_handle.get_all_asset_holders(asset_id)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/referrer_count', methods=['GET', 'POST'])
def get_referrer_count():
    header_origin = request.headers['Origin']
    account_id = request.args.get('account_id')
    ret_result, ret_data = impl_handle.get_referrer_count(account_id)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify([ret_data]))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_all_referrers', methods=['GET', 'POST'])
def get_all_referrers():
    header_origin = request.headers['Origin']
    account_id = request.args.get('account_id')
    page = request.args.get('page')
    ret_result, ret_data = impl_handle.get_all_referrers(account_id, page)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify([ret_data]))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_grouped_limit_orders', methods=['GET', 'POST'])
#todo 这个接口删除
def get_grouped_limit_orders():
    header_origin = request.headers['Origin']
    # base = request.args.get('base')
    # quote = request.args.get('quote')
    # group = request.args.get('group')
    # limit = request.args.get('limit')
    # ret_result, ret_data = impl_handle.get_grouped_limit_orders(base, quote, group, limit)
    # if not ret_result:
    ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


# *elastic*
@app.route('/get_account_history', methods=['GET', 'POST'])
def get_account_history_elastic():
    header_origin = request.headers['Origin']

    size = int(request.args.get('size'))
    from_ = int(request.args.get('from_'))

    ret_result, ret_data = impl_handle.get_account_history_elastic(size, from_)
    if not ret_result:
        ret_data = []

    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst

@app.route('/get_account_history2', methods=['GET', 'POST'])
def get_account_history_elastic2():
    header_origin = request.headers['Origin']
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    type = request.args.get('type')
    agg_field = request.args.get('agg_field')
    size = int(request.args.get('size'))

    if agg_field == 'operation_type':# 按op-type来获取op
        ret_result, ret_data = impl_handle.get_account_history_elastic2(from_date, to_date, type, agg_field, size)
    if agg_field == 'block_data.block_num': # todo 暂时以每个block中op最多来统计
        ret_result, ret_data = impl_handle.get_account_history_elastic3(from_date, to_date, type, agg_field, size)
    if agg_field == 'block_data.trx_id.keyword':# 根据每个交易的transactin中op最多数
        ret_result, ret_data = impl_handle.get_account_history_elastic4(from_date, to_date, type, agg_field, size)
    if not ret_result:
        ret_data = []
    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


@app.route('/get_trx', methods=['GET', 'POST'])
def get_trx():
    header_origin = request.headers['Origin']

    trx = request.args.get('trx')
    size = int(request.args.get('size'))
    sort = request.args.get('sort')
    ret_result, ret_data = impl_handle.get_trx(trx, size)

    rst = make_response(jsonify(ret_data))
    rst.headers['Content-Type'] = 'application/json'
    rst.headers['Access-Control-Allow-Origin'] = header_origin
    return rst


def create_instance():
    print('start syn...')
    handle = import_db()
    handle.run()


if __name__ == '__main__':
    t = threading.Thread(target=create_instance, args=())
    t.start()
    print("app.run")
    app.run(host='0.0.0.0', port=5000, threaded=True)

