"""SDK-level and application tests without Google credentials or a dataset."""

import copy
import json
from unittest.mock import Mock, patch

from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
import requests

from src.storage import BigQueryStore, DAY_MS, encode_rows
from src.synchronizer import Synchronizer
from tests.test_sync import OfflineTest, FIXTURES


class BigQueryTests(OfflineTest):
    def setUp(self):
        super().setUp()
        self.client = Mock(project='test-project')
        self.client.insert_rows_json.return_value = []
        self.client.create_table.side_effect = lambda table, **kwargs: table
        self.store = BigQueryStore(self.client, 'history')

    def test_table_layout(self):
        self.store.ensure_tables()
        calls = self.client.create_table.call_args_list
        self.assertEqual(len(calls), 2)
        for call in calls:
            table = call.args[0]
            self.assertEqual(table.time_partitioning.field, 'fetched_at')
            self.assertEqual(table.time_partitioning.type_, 'DAY')
            self.assertIn('account', table.clustering_fields)
            self.assertTrue(call.kwargs['exists_ok'])
            self.assertIsNone(call.kwargs['retry'])

    def test_existing_schema_mismatch_aborts_before_writing(self):
        def existing(table, **kwargs):
            table.schema = [bigquery.SchemaField('fetched_at', 'INT64')]
            return table
        self.client.create_table.side_effect = existing
        with self.assertRaisesRegex(ValueError, 'Incompatible schema'):
            self.store.ensure_tables()
        self.client.insert_rows_json.assert_not_called()

    def test_api_float_alias_is_accepted(self):
        def existing(table, **kwargs):
            table.schema = [bigquery.SchemaField(
                f.name, 'FLOAT' if f.field_type == 'FLOAT64' else f.field_type,
                mode=f.mode) for f in table.schema]
            return table
        self.client.create_table.side_effect = existing
        self.store.ensure_tables()

    def test_wrong_partitioning_is_rejected(self):
        def existing(table, **kwargs):
            table.time_partitioning = None
            return table
        self.client.create_table.side_effect = existing
        with self.assertRaisesRegex(ValueError, 'Incompatible partitioning'):
            self.store.ensure_tables()

    def test_conversion_drops_old_id_and_keeps_milliseconds(self):
        row = dict(id=42, account='test', symbol='BTC/USDT', size=1,
                   mark_price=50000, fetched_at=1700000000123)
        encoded = encode_rows('hist_positions', [row])[0]
        self.assertEqual(encoded['fetched_at'], '2023-11-14T22:13:20.123000+00:00')
        self.assertNotIn('id', encoded)
        self.assertEqual(row['fetched_at'], 1700000000123)

    def test_nullable_conversions(self):
        row = dict(account='test', currency='USD', collateral=1, fetched_at=0)
        result = encode_rows('hist_collaterals', [row])[0]
        self.assertIsNone(result['collateral_usd'])
        self.assertIsNone(result['collateral_jpy'])

    def test_empty_batch_does_not_send(self):
        self.store.insert_positions([])
        self.client.insert_rows_json.assert_not_called()

    def test_row_errors_fail_without_resubmission(self):
        self.client.insert_rows_json.return_value = [
            {'index': 0, 'errors': [{'message': 'private-response'}]},
        ]
        with self.assertRaises(RuntimeError) as caught:
            self.store.insert_collateral(dict(account='test', currency='USD',
                                              collateral=1, fetched_at=0))
        self.assertNotIn('private-response', str(caught.exception))
        self.client.insert_rows_json.assert_called_once()
        options = self.client.insert_rows_json.call_args.kwargs
        self.assertIsNone(options['retry'])
        self.assertEqual(options['row_ids'], bigquery.AutoRowIDs.DISABLED)

    def test_real_sdk_does_not_retry_transport_failure(self):
        # Exercise the actual SDK's request path, not a mock of insert_rows_json.
        client = bigquery.Client(project='test-project',
                                 credentials=AnonymousCredentials())
        self.addCleanup(client.close)
        store = BigQueryStore(client, 'history')
        with patch.object(client._http, 'request', side_effect=requests.ConnectionError) as request:
            with self.assertRaises(requests.ConnectionError):
                store.insert_collateral(dict(account='test', currency='USD',
                                              collateral=1, fetched_at=0))
        request.assert_called_once()

    def test_real_sdk_payload_has_no_insert_ids(self):
        client = bigquery.Client(project='test-project',
                                 credentials=AnonymousCredentials())
        self.addCleanup(client.close)
        response = requests.Response()
        response.status_code = 200
        response._content = b'{}'
        with patch.object(client._http, 'request', return_value=response) as request:
            BigQueryStore(client, 'history').insert_collateral(
                dict(account='test', currency='USD', collateral=1, fetched_at=0))
        payload = json.loads(request.call_args.kwargs['data'])
        self.assertIsNone(payload['rows'][0]['insertId'])
        self.assertEqual(payload['rows'][0]['json']['fetched_at'], '1970-01-01T00:00:00+00:00')

    def test_startup_query_is_parameterized(self):
        self.client.query.return_value.result.return_value = [
            {'symbol': 'BTC/USDT', 'last_seen': 123},
        ]
        self.assertEqual(self.store.recent_symbols("test'account", 100), {'BTC/USDT': 123})
        call = self.client.query.call_args
        self.assertNotIn("test'account", call.args[0])
        self.assertIn('fetched_at >= @since', call.args[0])
        self.assertEqual(call.kwargs['job_config'].query_parameters[0].value, "test'account")
        self.assertIsNone(call.kwargs['retry'])
        self.assertIsNone(call.kwargs['job_retry'])

    def test_invalid_identifiers_rejected(self):
        for dataset in ('history`', 'a.b', 'a;DROP TABLE x'):
            with self.assertRaises(ValueError):
                BigQueryStore(self.client, dataset)


class SynchronizerTests(OfflineTest):
    def make_sync(self, recent=None, **kwargs):
        store = Mock()
        store.recent_symbols.return_value = recent or {}
        sync = Synchronizer(Mock(), Mock(), store, Mock(), 'test', **kwargs)
        return sync, store

    def test_full_cycle(self):
        sync, store = self.make_sync()
        sync._client.id = 'binance'
        sync._client.fetch_positions.return_value = copy.deepcopy(FIXTURES['positions'])
        sync._client.fetch_ticker.return_value = {'last': 2000}
        sync._client.fapiPrivateV2GetAccount.return_value = FIXTURES['binance']
        fx = Mock()
        fx.fetch_ticker.return_value = {'last': 150}
        with patch('src.synchronizer.time.sleep'), \
             patch('src.synchronizer.time.time', return_value=1000), \
             patch('src.utils.ccxt.kraken', return_value=fx):
            sync._step()
        rows = store.insert_positions.call_args.args[0]
        collateral = store.insert_collateral.call_args.args[0]
        self.assertAlmostEqual(rows[0]['size'], 0.2)
        self.assertEqual(rows[1]['mark_price'], 2000)
        self.assertEqual(collateral['collateral_jpy'], 180075)
        self.assertEqual({r['fetched_at'] for r in rows}, {collateral['fetched_at']})
        self.assertEqual(collateral['fetched_at'], 1000000)

    def test_cache_restoration_update_and_expiry(self):
        now = DAY_MS + 1000
        sync, store = self.make_sync({'recent': now - 1, 'expired': 0})
        def positions(*_):
            return [dict(symbol=s, size=size, mark_price=1) for s, size in
                    [('recent', 0), ('expired', 0), ('unused', 0), ('new', 1)]]
        with patch('src.synchronizer.fetch_positions', side_effect=positions), \
             patch('src.synchronizer.time.sleep'):
            sync._fetch_hist_positions(now)
            self.assertEqual({r['symbol'] for r in store.insert_positions.call_args.args[0]},
                             {'recent', 'new'})
            self.assertEqual(sync._recent_symbols['new'], now)
            sync._fetch_hist_positions(now + DAY_MS + 1)
            self.assertEqual({r['symbol'] for r in store.insert_positions.call_args.args[0]}, {'new'})
        store.recent_symbols.assert_called_once()

    def test_failed_cycle_is_not_replayed_and_health_only_pings_success(self):
        sync, store = self.make_sync()
        sync._health_check_ping = Mock(side_effect=KeyboardInterrupt)
        sync._loop_interval = 0
        store.insert_positions.side_effect = [requests.ConnectionError, None]
        def positions(*_):
            return [dict(symbol='BTC/USDT', size=1, mark_price=50000)]
        with patch('src.synchronizer.time.sleep'), \
             patch('src.synchronizer.time.time', side_effect=[1000, 2000]), \
             patch('src.synchronizer.fetch_positions', side_effect=positions), \
             patch('src.synchronizer.fetch_collateral', return_value={'collateral': 1, 'currency': 'USD'}), \
             patch('src.synchronizer.fetch_converted_collaterals', return_value={'usd': 1, 'jpy': 150}):
            with self.assertRaises(KeyboardInterrupt):
                sync.run()
        sent = [call.args[0][0]['fetched_at'] for call in store.insert_positions.call_args_list]
        self.assertEqual(sent, [1000000, 2000000])
        store.insert_collateral.assert_called_once()
        sync._health_check_ping.assert_called_once()
        store.recent_symbols.assert_called_once()
