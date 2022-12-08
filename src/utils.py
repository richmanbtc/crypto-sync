import ccxt


def create_ccxt_client(exchange, api_key=None, api_secret=None,
                       api_password=None, subaccount=None):
    headers = {}
    options = {}

    if exchange == 'ftx' and subaccount is not None and subaccount != '':
        headers['FTX-SUBACCOUNT'] = subaccount
    if exchange == 'binance':
        options['defaultType'] = 'future'

    client = getattr(ccxt, exchange)({
        'apiKey': api_key,
        'secret': api_secret,
        'password': api_password,
        'headers': headers,
        'options': options,
    })

    return client


def fetch_orders(client, symbol):
    if client.id == 'okx':
        client.load_markets()
        market = client.market(symbol)
        type, query = client.handle_market_type_and_params('fetchCanceledOrders', market, {})
        res = client.privateGetTradeOrdersHistory({
            'instType': client.convert_to_instrument_type(type),
            'instId': market['id'],
        })
        data = client.safe_value(res, 'data', [])
        return client.parse_orders(data, market, None, None)

    return client.fetch_orders(symbol=symbol)


def fetch_collateral(client):
    if client.id == 'binance':
        res = client.fapiPrivateGetAccount()
        collateral = float(res['totalMarginBalance'])
        currency = 'USD'
    elif client.id == 'bybit':
        res = client.privateGetV2PrivateWalletBalance({ 'coin': 'USDT' })
        collateral = float(res['result']['USDT']['equity'])
        currency = 'USD'
    elif client.id == 'okx':
        res = client.privateGetAccountBalance()
        collateral = float(res['data'][0]['totalEq'])
        currency = 'USD'
    elif client.id == 'bitflyer':
        res = client.privateGetGetcollateral()
        collateral = float(res['collateral']) + float(res['open_position_pnl'])
        currency = 'JPY'
    else:
        raise Exception('not implemented')

    return dict(collateral=collateral, currency=currency)
