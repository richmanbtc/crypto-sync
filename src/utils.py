import ccxt


def create_ccxt_client(exchange, api_key=None, api_secret=None,
                       api_password=None, subaccount=None, account_type=None):
    headers = {}
    options = {}

    if exchange == 'ftx' and subaccount is not None and subaccount != '':
        headers['FTX-SUBACCOUNT'] = subaccount
    if exchange == 'binance':
        options['defaultType'] = 'future'
    if exchange == 'bybit':
        if account_type is not None:
            options['defaultSubType'] = 'inverse'

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


def fetch_collateral(client, account_type):
    if client.id == 'binance':
        res = client.fapiPrivateV2GetAccount()
        collateral = float(res['totalMarginBalance'])
        currency = 'USD'
    elif client.id == 'bybit':
        coin = {
            None: 'USDT',
            'btc': 'BTC',
            'eth': 'ETH',
        }[account_type]
        res = client.privateGetV5AccountWalletBalance({
            'accountType': 'CONTRACT',
            'coin': coin,
        })
        collateral = float(res['result']['list'][0]['coin'][0]['equity'])
        currency = 'USD' if coin == 'USDT' else coin
    elif client.id == 'okx':
        res = client.privateGetAccountBalance()
        collateral = float(res['data'][0]['totalEq'])
        currency = 'USD'
    elif client.id == 'kucoinfutures':
        res = client.futuresPrivateGetAccountOverview({
            'currency': 'USDT'
        })
        collateral = float(res['data']['accountEquity'])
        currency = 'USD'
    elif client.id == 'bitflyer':
        res = client.privateGetGetcollateral()
        collateral = float(res['collateral']) + float(res['open_position_pnl'])
        currency = 'JPY'
    else:
        raise Exception('not implemented')

    return dict(collateral=collateral, currency=currency)


def fetch_converted_collaterals(collateral, currency):
    client = ccxt.kraken()
    if currency == 'JPY':
        price = client.fetch_ticker('USD/JPY')['last']
        return dict(jpy=collateral, usd=collateral / price)
    elif currency == 'USD':
        price = client.fetch_ticker('USD/JPY')['last']
        return dict(jpy=collateral * price, usd=collateral)

    price = client.fetch_ticker('{}/USD'.format(currency))['last']
    return fetch_converted_collaterals(collateral * price, 'USD')


def fetch_positions(client, account_type):
    if client.id == 'bitflyer':
        res = client.privateGetGetpositions({'product_code': 'FX_BTC_JPY'})
        pos = 0.0
        for item in res:
            pos += float(item['size']) * (1 if item['side'] == 'BUY' else -1)
        return [
            {
                'symbol': 'BTC/JPY:JPY',
                'size': pos,
                'mark_price': None,
            }
        ]

    symbols = None
    if client.id == 'bybit':
        if account_type is not None:
            symbols = {
                'btc': 'BTC/USD:BTC',
                'eth': 'ETH/USD:ETH',
            }[account_type]
            symbols = [symbols]

    positions = client.fetch_positions(symbols=symbols)
    return _merge_positions(positions)


def _merge_positions(positions):
    merged = {}
    for pos in positions:
        symbol = pos['symbol']
        if symbol not in merged:
            merged[symbol] = {
                'symbol': symbol,
                'size': 0.0,
                'mark_price': pos['markPrice'],
            }
        side_int = 1 if pos['side'] == 'long' else -1
        merged[symbol]['size'] += pos['contracts'] * pos['contractSize'] * side_int
    return list(merged.values())


def validate_account_type(exchange, account_type):
    if exchange == 'bybit':
        allowed_account_types = [None, 'btc', 'eth']
    else:
        allowed_account_types = [None]

    if account_type not in allowed_account_types:
        raise Exception('invalid account_type {} {}'.format(exchange, account_type))
