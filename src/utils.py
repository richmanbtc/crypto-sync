import ccxt


def create_ccxt_client(exchange, api_key=None, api_secret=None,
                       api_password=None, subaccount=None):
    headers = {}
    options = {}

    if exchange == 'ftx' and subaccount is not None and subaccount != '':
        headers['FTX-SUBACCOUNT'] = subaccount
    if exchange == 'binance':
        options['defaultType'] = 'future'
        # Futures collection does not need the authenticated SAPI currency API.
        options['fetchCurrencies'] = False

    client = getattr(ccxt, exchange)({
        'apiKey': api_key,
        'secret': api_secret,
        'password': api_password,
        'headers': headers,
        'options': options,
    })

    return client


def fetch_collateral(client):
    if client.id == 'binance':
        res = client.fapiPrivateV2GetAccount()
        collateral = float(res['totalMarginBalance'])
        currency = 'USD'
    elif client.id == 'bybit':
        res = client.privateGetV5AccountWalletBalance({
            'accountType': 'UNIFIED',
            'coin': 'USDT',
        })
        collateral = float(res['result']['list'][0]['coin'][0]['equity'])
        currency = 'USD'
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


def fetch_positions(client):
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

    positions = client.fetch_positions()
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
