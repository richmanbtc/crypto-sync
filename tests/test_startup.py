"""Startup failures must stay cheap, private, and interruptible."""

import os
import select
import signal
import subprocess
import sys
import unittest
from unittest.mock import patch

from src.main import main


class StartupTests(unittest.TestCase):
    def test_bad_settings_need_no_installed_sdks(self):
        # -S excludes site-packages; even main's import must work without SDKs.
        script = '''
import os
import sys
import unittest
from unittest.mock import patch
from src.main import main

base = dict(CCXT_EXCHANGE='bybit', CRYPTO_SYNC_ACCOUNT='test',
            CRYPTO_SYNC_BQ_PROJECT='test-project', CRYPTO_SYNC_BQ_DATASET='history')
cases = [({}, 'Required settings missing')]
for field in ('CRYPTO_SYNC_PANIC_INTERVAL', 'CRYPTO_SYNC_ACCOUNT_TYPE',
              'CRYPTO_SYNC_LOG_LEVEL'):
    cases.append((dict(base, **{field: 'synthetic-private-value'}), field))
for env, expected in cases:
    with patch.dict(os.environ, env, clear=True), \\
         patch('src.main.time.sleep') as sleep, \\
         patch('src.main.random.uniform', return_value=7):
        with unittest.TestCase().assertLogs('crypto_sync') as logs:
            assert main() == 1
        assert expected in str(logs.output)
        assert 'synthetic-private-value' not in str(logs.output)
        sleep.assert_called_once_with(7)
assert not any(name.split('.')[0] in ('ccxt', 'google') for name in sys.modules)
'''
        result = subprocess.run([sys.executable, '-S', '-c', script], env={},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(os.name == 'posix', 'Requires POSIX signals')
    def test_sigterm_interrupts_real_startup_delay(self):
        proc = subprocess.Popen([sys.executable, '-S', '-m', 'src.main'],
                                env={}, stdout=subprocess.DEVNULL,
                                stderr=subprocess.PIPE, text=True)
        try:
            ready, _, _ = select.select([proc.stderr], [], [], 10)
            self.assertTrue(ready, 'Startup did not log its failure')
            line = proc.stderr.readline()
            for name in ('CCXT_EXCHANGE', 'CRYPTO_SYNC_ACCOUNT',
                         'CRYPTO_SYNC_BQ_PROJECT', 'CRYPTO_SYNC_BQ_DATASET'):
                self.assertIn(name, line)
            self.assertIn('exiting in', line)
            self.assertIsNone(proc.poll())
            proc.send_signal(signal.SIGTERM)
            self.assertEqual(proc.wait(timeout=3), 0)
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            proc.stderr.close()

    def test_success_does_not_delay(self):
        with patch('src.main.start'), patch('src.main.time.sleep') as sleep:
            self.assertEqual(main(), 0)
        sleep.assert_not_called()
