import logging
import os
import threading
import time


class PanicManager:
    def __init__(self, logger=None):
        self.monitors = {}
        self.logger = logger or logging.getLogger(__name__)
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def register(self, tag=None, start_time=None, interval=None):
        with self.lock:
            self.monitors[tag] = {
                'start_at': time.monotonic(), 'ping_at': None,
                'start_time': start_time, 'interval': interval,
            }

    def ping(self, tag=None):
        with self.lock:
            self.monitors[tag]['ping_at'] = time.monotonic()

    def panic(self):
        os._exit(1)

    def check(self):
        now = time.monotonic()
        with self.lock:
            for tag, monitor in self.monitors.items():
                pinged = monitor['ping_at'] is not None
                last = monitor['ping_at'] if pinged else monitor['start_at']
                timeout = monitor['interval'] if pinged else monitor['start_time']
                if now - last > timeout:
                    self.logger.error('%s health check delayed; exiting', tag)
                    self.panic()

    def run(self):
        while True:
            time.sleep(5)
            self.check()
