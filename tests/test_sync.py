"""Offline regression tests. All account and market values are synthetic."""

import copy
import importlib
import json
import logging
from pathlib import Path
import unittest
from unittest.mock import Mock, patch


from src import utils
from src.logger import create_logger, failure_location
from src.main import load_settings
from src.panic_manager import PanicManager
from src.storage import BigQueryStore
from src.synchronizer import Synchronizer

FIXTURES = json.loads((Path(__file__).parent / 'fixtures/exchanges.json').read_text())


class OfflineTest(unittest.TestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'socket.socket.connect_ex',
                       'socket.create_connection', 'socket.getaddrinfo'):
            blocker = patch(target, side_effect=AssertionError('Network is forbidden'))
            blocker.start()
            self.addCleanup(blocker.stop)


class ExchangeTests(OfflineTest):
    def test_private_request_signing_with_synthetic_credentials(self):
        cases = (
            ('binance', 'account', 'fapiPrivateV2'),
            ('bybit', 'v5/account/wallet-balance', 'private'),
            ('okx', 'account/balance', 'private'),
            ('kucoinfutures', 'account-overview', 'futuresPrivate'),
            ('bitflyer', 'getcollateral', 'private'),
        )
        for exchange, path, api in cases:
            with self.subTest(exchange=exchange):
                client = utils.create_ccxt_client(
                    exchange, api_key='synthetic-key',
                    api_secret='synthetic-secret', api_password='synthetic-passphrase',
                )
                request = client.sign(path, api, 'GET', {})
                self.assertTrue(request['url'].startswith('https://'))
                self.assertTrue(request['headers'])
                self.assertNotIn('synthetic-secret', str(request))

    def test_collateral_for_all_exchanges(self):
        methods = {
            'binance': 'fapiPrivateV2GetAccount',
            'okx': 'privateGetAccountBalance',
            'kucoinfutures': 'futuresPrivateGetAccountOverview',
            'bitflyer': 'privateGetGetcollateral',
        }
        for exchange, method in methods.items():
            with self.subTest(exchange=exchange):
                client = Mock(id=exchange)
                getattr(client, method).return_value = FIXTURES[exchange]
                result = utils.fetch_collateral(client)
                self.assertEqual(result, {
                    'collateral': 1200.5,
                    'currency': 'JPY' if exchange == 'bitflyer' else 'USD',
                })

    def test_bybit_unified_collateral(self):
        client = Mock(id='bybit')
        client.privateGetV5AccountWalletBalance.return_value = FIXTURES['bybit']
        self.assertEqual(utils.fetch_collateral(client), {
            'collateral': 12.5, 'currency': 'USD',
        })
        client.privateGetV5AccountWalletBalance.assert_called_once_with({
            'accountType': 'UNIFIED', 'coin': 'USDT',
        })

    def test_binance_market_loading_skips_private_currency_api(self):
        client = utils.create_ccxt_client('binance')
        with patch.object(client, 'check_required_credentials', return_value=True), \
             patch.object(client, 'sapiGetCapitalConfigGetall') as currencies, \
             patch.object(client, 'fetch_markets', return_value=[]):
            client.load_markets()
        currencies.assert_not_called()

    def test_position_netting_and_missing_price(self):
        rows = copy.deepcopy(FIXTURES['positions'])
        merged = utils._merge_positions(rows)
        self.assertAlmostEqual(merged[0]['size'], 0.2)
        self.assertEqual(merged[1]['size'], -2)
        self.assertIsNone(merged[1]['mark_price'])
        self.assertEqual(rows, FIXTURES['positions'])
        self.assertEqual(utils._merge_positions([]), [])

    def test_bitflyer_positions(self):
        client = Mock(id='bitflyer')
        client.privateGetGetpositions.return_value = FIXTURES['bitflyer_positions']
        result = utils.fetch_positions(client)
        self.assertAlmostEqual(result[0]['size'], 0.3)
        self.assertIsNone(result[0]['mark_price'])
        client.privateGetGetpositions.assert_called_once_with({'product_code': 'FX_BTC_JPY'})

    def test_bybit_positions(self):
        client = Mock(id='bybit')
        client.fetch_positions.return_value = []
        self.assertEqual(utils.fetch_positions(client), [])
        client.fetch_positions.assert_called_once_with()

    def test_currency_conversion(self):
        client = Mock()
        client.fetch_ticker.side_effect = lambda symbol: {
            'last': {'USD/JPY': 150, 'BTC/USD': 50000}[symbol]
        }
        with patch('src.utils.ccxt.kraken', return_value=client):
            self.assertEqual(utils.fetch_converted_collaterals(300, 'JPY'),
                             {'jpy': 300, 'usd': 2})
            self.assertEqual(utils.fetch_converted_collaterals(2, 'USD'),
                             {'jpy': 300, 'usd': 2})
            self.assertEqual(utils.fetch_converted_collaterals(1, 'BTC'),
                             {'jpy': 7500000, 'usd': 50000})

    def test_exchange_options_without_api_calls(self):
        client = utils.create_ccxt_client('binance')
        self.assertEqual(client.options['defaultType'], 'future')


class RuntimeTests(OfflineTest):
    def test_main_import_does_not_start_application(self):
        import src.main
        with patch('google.cloud.bigquery.Client') as connect:
            importlib.reload(src.main)
        connect.assert_not_called()

    def test_startup_failure_logs_no_private_exception_body(self):
        from src.main import main
        with patch('src.main.start', side_effect=RuntimeError('synthetic-private-response')), \
             patch('src.main.create_logger') as logger, \
             patch('src.main.random.uniform', return_value=7) as delay, \
             patch('src.main.time.sleep') as sleep:
            self.assertEqual(main(), 1)
        delay.assert_called_once_with(5, 10)
        sleep.assert_called_once_with(7)
        calls = str(logger.return_value.mock_calls)
        self.assertIn('RuntimeError', calls)
        self.assertNotIn('synthetic-private-response', calls)

    def test_settings_defaults_and_validation(self):
        env = {'CCXT_EXCHANGE': 'bybit', 'CRYPTO_SYNC_ACCOUNT': 'test',
               'CRYPTO_SYNC_BQ_PROJECT': 'test-project', 'CRYPTO_SYNC_BQ_DATASET': 'history'}
        self.assertEqual(load_settings(env)['panic_interval'], 300)
        self.assertEqual(load_settings(env)['log_level'], 'INFO')
        for name in env:
            with self.subTest(missing=name), self.assertRaises(ValueError):
                load_settings({k: v for k, v in env.items() if k != name})
        for interval in ('0', '-1', 'invalid'):
            with self.subTest(interval=interval), self.assertRaises(ValueError):
                load_settings(dict(env, CRYPTO_SYNC_PANIC_INTERVAL=interval))

    def test_failure_location_excludes_private_values(self):
        try:
            raise RuntimeError('synthetic-private-payload')
        except RuntimeError as error:
            location = failure_location(error)
        self.assertIn('test_sync.py:', location)
        self.assertIn('test_failure_location_excludes_private_values', location)
        self.assertNotIn('synthetic-private-payload', location)
        self.assertNotIn(str(Path(__file__).parent), location)

    def test_logger_is_idempotent(self):
        logger = create_logger(None)
        count = len(logger.handlers)
        self.assertIs(create_logger('DEBUG'), logger)
        self.assertEqual(len(logger.handlers), count)
        self.assertEqual(logger.level, logging.DEBUG)
        with self.assertRaises(ValueError):
            create_logger('invalid')

    def test_loop_retries_and_pings_only_on_success(self):
        ping = Mock(side_effect=KeyboardInterrupt)
        logger = Mock()
        store = Mock()
        store.recent_symbols.return_value = {}
        sync = Synchronizer(Mock(), logger, store, ping, 'test')
        sync._loop_interval = 0
        sync._step = Mock(side_effect=[RuntimeError('synthetic private payload'), None])
        with self.assertRaises(KeyboardInterrupt):
            sync.run()
        self.assertEqual(sync._step.call_count, 2)
        ping.assert_called_once()
        self.assertNotIn('synthetic private payload', str(logger.mock_calls))

    def test_watchdog_deadlines(self):
        with patch('src.panic_manager.threading.Thread.start'):
            manager = PanicManager(logger=Mock())
        with patch('src.panic_manager.time.monotonic', return_value=0), \
             patch.object(manager, 'panic') as panic:
            manager.register('bot', 10, 20)
            manager.check()
            panic.assert_not_called()
            manager.ping('bot')
            with patch('src.panic_manager.time.monotonic', return_value=19):
                manager.check()
                panic.assert_not_called()
            with patch('src.panic_manager.time.monotonic', return_value=21):
                manager.check()
                panic.assert_called_once()

    def test_watchdog_startup_timeout(self):
        with patch('src.panic_manager.threading.Thread.start'):
            manager = PanicManager(logger=Mock())
        with patch('src.panic_manager.time.monotonic', return_value=0):
            manager.register('bot', 10, 20)
        with patch('src.panic_manager.time.monotonic', return_value=11), \
             patch.object(manager, 'panic') as panic:
            manager.check()
            panic.assert_called_once()



if __name__ == '__main__':
    unittest.main()
