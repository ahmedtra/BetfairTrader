import threading
from time import sleep

from betfair.betfair import Betfair

from common import get_config, get_certif_path, singleton

from structlog import get_logger


client = None
client_lock = threading.Lock()

def authenticate():
    conf = get_config()
    username = conf.get("auth", "username")
    password = conf.get("auth", "pass")

    app_key = conf.get("auth", "appkey")
    certif = get_certif_path()

    get_logger().info("connecting to betfair api")

    client = Betfair(app_key, certif, timeout=60)
    client.login(username, password)

    get_logger().info("connected to the betfair api")

    return client

class client_manager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()

    def run(self):
        while True:
            sleep(600)
            get_client().keep_alive()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

@singleton
def get_client(reconnect = False):
    global client
    global client_lock
    with client_lock:
        if client is None or reconnect:
            client = authenticate()
    return client




