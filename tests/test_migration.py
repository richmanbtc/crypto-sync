from unittest.mock import MagicMock, Mock

from src.migrate_postgres import migrate
from tests.test_sync import OfflineTest


class MigrationTests(OfflineTest):
    def connection(self):
        connection = Mock()
        positions = MagicMock()
        collaterals = MagicMock()
        positions.__enter__.return_value = positions
        collaterals.__enter__.return_value = collaterals
        positions.fetchmany.side_effect = [
            [('test', 'BTC/USDT', 1, 50000, 123)],
            [('test', 'ETH/USDT', -1, 2000, 456)], [],
        ]
        collaterals.fetchmany.side_effect = [[('test', 'USD', 1, None, None, 123)], []]
        connection.cursor.side_effect = [positions, collaterals]
        return connection, positions, collaterals

    def test_batches_are_read_only_and_preserve_history(self):
        connection, positions, _ = self.connection()
        store = Mock()
        counts = migrate(connection, store, Mock(), batch_size=1)
        connection.set_session.assert_called_once_with(readonly=True)
        self.assertEqual(counts, {'hist_positions': 2, 'hist_collaterals': 1})
        self.assertEqual(store.load_history.call_count, 3)
        first = store.load_history.call_args_list[0]
        self.assertEqual(first.args[0], 'hist_positions')
        self.assertEqual(first.args[1][0]['fetched_at'], 123)
        self.assertNotIn('id', first.args[1][0])
        positions.__exit__.assert_called_once()

    def test_load_failure_stops_without_retry_or_next_table(self):
        connection, positions, collaterals = self.connection()
        store = Mock()
        store.load_history.side_effect = RuntimeError
        with self.assertRaises(RuntimeError):
            migrate(connection, store, Mock())
        store.load_history.assert_called_once()
        collaterals.fetchmany.assert_not_called()
        positions.__exit__.assert_called_once()

    def test_invalid_batch_size_does_not_connect(self):
        connection = Mock()
        with self.assertRaises(ValueError):
            migrate(connection, Mock(), Mock(), 0)
        connection.set_session.assert_not_called()
