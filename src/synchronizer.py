import time

from .logger import failure_location
from .storage import DAY_MS
from .utils import (
    fetch_collateral,
    fetch_positions,
    fetch_converted_collaterals,
)


class Synchronizer:
    def __init__(self, client, logger,
                 store, health_check_ping, account, account_type):
        self._client = client
        self._logger = logger
        self._store = store
        self._recent_symbols = store.recent_symbols(
            account, int(time.time() * 1000) - DAY_MS
        )
        self._health_check_ping = health_check_ping
        self._account = account
        self._account_type = account_type
        self._fetch_interval = 2
        self._loop_interval = 60

    def run(self):
        while True:
            try:
                self._step()
                self._health_check_ping()
            except Exception as error:
                # API/DB exception text can contain private response or connection data.
                self._logger.error(
                    'Synchronization failed (%s) at %s; discarding cycle data',
                    type(error).__name__, failure_location(error),
                )
            time.sleep(self._loop_interval)

    def _step(self):
        fetched_at = int(time.time() * 1000)
        self._fetch_hist_positions(fetched_at)
        self._fetch_hist_collaterals(fetched_at)

    def _fetch_hist_positions(self, fetched_at):
        self._fetch_sleep()
        self._logger.info('fetch_positions')
        positions = fetch_positions(self._client, self._account_type)
        self._recent_symbols = {
            symbol: seen for symbol, seen in self._recent_symbols.items()
            if seen >= fetched_at - DAY_MS
        }
        for position in positions:
            if position['size'] != 0:
                self._recent_symbols[position['symbol']] = fetched_at
        for i in range(len(positions))[::-1]:
            pos = positions[i]
            if pos['size'] == 0 and pos['symbol'] not in self._recent_symbols:
                positions.pop(i)
                continue
            if pos['mark_price'] is None:
                self._fetch_sleep()
                ticker = self._client.fetch_ticker(pos['symbol'])
                pos['mark_price'] = ticker['last']
        self._add_common_columns(positions, fetched_at)
        self._logger.info('Saving %d positions', len(positions))
        self._store.insert_positions(positions)

    def _fetch_hist_collaterals(self, fetched_at):
        self._fetch_sleep()
        self._logger.info('fetch_collateral')
        result = fetch_collateral(self._client, self._account_type)
        converted = fetch_converted_collaterals(result['collateral'], result['currency'])
        for key in converted:
            result['collateral_{}'.format(key)] = converted[key]
        self._add_common_columns([result], fetched_at)
        self._logger.info('Saving collateral')
        self._store.insert_collateral(result)

    def _fetch_sleep(self):
        time.sleep(self._fetch_interval)

    def _add_common_columns(self, rows, fetched_at):
        for row in rows:
            row['account'] = self._account
            row['fetched_at'] = fetched_at

