import time
import traceback
from .utils import (
    fetch_collateral,
    fetch_orders,
    fetch_positions,
    fetch_converted_collaterals,
)


class Synchronizer:
    def __init__(self, client, logger,
                 db, health_check_ping, account, account_type):
        self._client = client
        self._logger = logger
        self._db = db
        self._health_check_ping = health_check_ping
        self._account = account
        self._account_type = account_type
        self._fetch_interval = 2
        self._loop_interval = 60

        self._orders_table = create_orders_table(self._db)
        self._hist_positions_table = create_hist_positions_table(self._db)
        self._hist_collaterals_table = create_hist_collaterals_table(self._db)

    def run(self):
        while True:
            try:
                self._step()
                self._health_check_ping()
            except Exception as e:
                self._logger.error(e)
                self._logger.error(traceback.format_exc())

            time.sleep(self._loop_interval)

    def _step(self):
        fetched_at = int(time.time() * 1000)
        self._fetch_orders(fetched_at)
        self._fetch_hist_positions(fetched_at)
        self._fetch_hist_collaterals(fetched_at)

    def _fetch_orders(self, fetched_at):
        statement = "SELECT DISTINCT symbol FROM hist_positions WHERE account = :account"
        symbols = [row['symbol'] for row in self._db.query(statement, account=self._account)]

        for symbol in symbols:
            self._fetch_sleep()
            self._logger.info('fetch_orders {}'.format(symbol))
            orders = fetch_orders(self._client, symbol)
            orders = list(map(normalize_order, orders))
            self._add_common_columns(orders, fetched_at)

            ids = [x['order_id'] for x in orders]
            statement = """SELECT order_id FROM orders
            WHERE account = :account AND symbol = :symbol
            AND status <> 'open' AND order_id = ANY(:ids)"""
            frozen_order_ids = set([row['order_id'] for row in self._db.query(
                statement, account=self._account, symbol=symbol, ids=ids)])

            orders = [x for x in orders if x['order_id'] not in frozen_order_ids]
            self._logger.info('upsert {}'.format(orders))
            self._orders_table.upsert_many(orders, keys=['account', 'order_id'])

    def _fetch_hist_positions(self, fetched_at):
        self._fetch_sleep()
        self._logger.info('fetch_positions')
        positions = fetch_positions(self._client, self._account_type)
        statement = "SELECT DISTINCT symbol FROM hist_positions WHERE account = :account"
        existing_symbols = set([row['symbol'] for row in self._db.query(statement, account=self._account)])
        for i in range(len(positions))[::-1]:
            pos = positions[i]
            if pos['size'] == 0 and pos['symbol'] not in existing_symbols:
                positions.pop(i)
                continue
            if pos['mark_price'] is None:
                self._fetch_sleep()
                ticker = self._client.fetch_ticker(pos['symbol'])
                pos['mark_price'] = ticker['last']
        self._add_common_columns(positions, fetched_at)
        self._logger.info('insert {}'.format(positions))
        self._hist_positions_table.insert_many(positions)

    def _fetch_hist_collaterals(self, fetched_at):
        self._fetch_sleep()
        self._logger.info('fetch_collateral')
        result = fetch_collateral(self._client, self._account_type)
        converted = fetch_converted_collaterals(result['collateral'], result['currency'])
        for key in converted:
            result['currency_{}'.format(key)] = converted[key]
        self._add_common_columns([result], fetched_at)
        self._logger.info('insert {}'.format(result))
        self._hist_collaterals_table.insert(result)

    def _fetch_sleep(self):
        time.sleep(self._fetch_interval)

    def _add_common_columns(self, rows, fetched_at):
        for row in rows:
            row['account'] = self._account
            row['fetched_at'] = fetched_at


def create_orders_table(db):
    table = db.create_table(
        'orders',
    )
    table.create_column('account', db.types.guess('account'), nullable=False)
    table.create_column('order_id', db.types.guess('order'), nullable=False)
    table.create_column('timestamp', db.types.guess(1))
    table.create_column('last_trade_timestamp', db.types.guess(1))
    table.create_column('status', db.types.guess('open'))
    table.create_column('symbol', db.types.guess('BTC/USDT'), nullable=False)
    table.create_column('type', db.types.guess('limit'))
    table.create_column('time_in_force', db.types.guess('GTC'))
    table.create_column('is_buy', db.types.guess(True))
    table.create_column('price', db.types.guess(1.2))
    table.create_column('average', db.types.guess(1.2))
    table.create_column('amount', db.types.guess(1.2))
    table.create_column('filled', db.types.guess(1.2))
    table.create_column('remaining', db.types.guess(1.2))
    table.create_column('cost', db.types.guess(1.2))
    table.create_column('fetched_at', db.types.guess(1), nullable=False)

    table.create_index(['fetched_at', 'account'])
    table.create_index(['account', 'order_id'], unique=True)

    return table


def normalize_order(row):
    # https://github.com/ccxt/ccxt/wiki/Manual#order-structure
    return {
        'order_id': row['id'],
        'timestamp': row['timestamp'],
        'last_trade_timestamp': row['lastTradeTimestamp'],
        'status': row['status'],
        'symbol': row['symbol'],
        'type': row['type'],
        'time_in_force': row['timeInForce'],
        'is_buy': row['side'] == 'buy',
        'price': row['price'],
        'average': row['average'],
        'amount': row['amount'],
        'filled': row['filled'],
        'remaining': row['remaining'],
        'cost': row['cost'],
    }


def create_hist_positions_table(db):
    table = db.create_table(
        'hist_positions',
    )
    table.create_column('account', db.types.guess('account'), nullable=False)
    table.create_column('symbol', db.types.guess('BTC/USDT'), nullable=False)
    table.create_column('size', db.types.guess(1.2), nullable=False)
    table.create_column('mark_price', db.types.guess(1.2), nullable=False)
    table.create_column('fetched_at', db.types.guess(1), nullable=False)

    table.create_index(['fetched_at', 'account', 'symbol'], unique=True)

    return table


def create_hist_collaterals_table(db):
    table = db.create_table(
        'hist_collaterals',
    )
    table.create_column('account', db.types.guess('account'), nullable=False)
    table.create_column('currency', db.types.guess('USD'), nullable=False)
    table.create_column('collateral', db.types.guess(1.2), nullable=False)
    table.create_column('fetched_at', db.types.guess(1), nullable=False)
    table.create_column('collateral_jpy', db.types.guess(1.2), nullable=True)
    table.create_column('collateral_usd', db.types.guess(1.2), nullable=True)

    table.create_index(['fetched_at', 'account'], unique=True)

    return table

